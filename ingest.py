import copy
import hashlib
import json
import time
import uuid
from datetime import datetime, timezone
from urllib.parse import urlsplit


TARGET_SCHEMA = "QUERY_API_STARTER.INGEST"


class ConfigurationError(Exception):
    pass


class InvalidPage(Exception):
    pass


class RetryableError(Exception):
    def __init__(self, message, retry_after_seconds=0):
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


class FatalAPIError(Exception):
    pass


def decode(value):
    return json.loads(value) if isinstance(value, str) else value


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def validate_config(config):
    if config.get("contract_confirmed") is not True:
        raise ConfigurationError("Confirm the API request contract before enabling ingestion")
    for key in ("page_size", "max_pages_per_invocation", "max_response_bytes", "new_run_interval_seconds"):
        if type(config.get(key)) is not int or config[key] <= 0:
            raise ConfigurationError("Invalid positive integer setting: " + key)
    if not 60 <= config.get("invocation_budget_seconds", 0) <= 240:
        raise ConfigurationError("Invocation budget must be between 60 and 240 seconds")
    request = config.get("request", {})
    if request.get("method") not in ("GET", "POST"):
        raise ConfigurationError("Request method must be GET or POST")
    if request.get("pagination_location") not in ("params", "json"):
        raise ConfigurationError("Pagination location must be params or json")
    if request["method"] == "GET" and request["pagination_location"] == "json":
        raise ConfigurationError("GET pagination must use query parameters")
    if not request.get("page_parameter") or not request.get("size_parameter"):
        raise ConfigurationError("Pagination parameter names are required")
    if request["page_parameter"] == request["size_parameter"]:
        raise ConfigurationError("Page and size parameter names must differ")
    for key in ("params", "json"):
        if not isinstance(request.get(key), dict):
            raise ConfigurationError("Request params and json must be objects")
    auth = config.get("auth", {})
    if auth.get("mode") not in ("basic", "direct_pat", "exchange"):
        raise ConfigurationError("Choose basic, direct_pat or exchange authentication")
    if auth["mode"] == "basic":
        if not isinstance(auth.get("username"), str) or not auth["username"] or ":" in auth["username"]:
            raise ConfigurationError("Basic authentication requires a username without a colon")
    elif not auth.get("header"):
        raise ConfigurationError("Authentication header name is required")
    urls = [request.get("url", "")]
    if auth["mode"] == "exchange":
        if not auth.get("pat_json_field") and not auth.get("pat_header"):
            raise ConfigurationError("Configure the exchange PAT field or header")
        if not isinstance(auth.get("exchange_body"), dict) or not auth.get("token_field"):
            raise ConfigurationError("Configure exchange body and top-level token field")
        urls.append(auth.get("exchange_url", ""))
    allowed = config.get("allowed_hosts", [])
    if not allowed or any("example" in host or "REPLACE" in host for host in allowed):
        raise ConfigurationError("Replace the allowed API hosts")
    for url in urls:
        parsed = urlsplit(url)
        if (parsed.scheme != "https" or parsed.hostname not in allowed
                or parsed.username or parsed.password or parsed.fragment or parsed.query
                or parsed.port not in (None, 443)):
            raise ConfigurationError("URLs must use approved HTTPS hosts on port 443 without credentials or query strings")
    if request["method"] == "POST" and config.get("post_is_read_only") is not True:
        raise ConfigurationError("Confirm POST is a read-only query before permitting retries")


def validate_page(payload, expected_page, previous_pages=None, previous_total=None, previous_size=None):
    if not isinstance(payload, dict) or not isinstance(payload.get("content"), list):
        raise InvalidPage("Response must contain a content array")
    for key in ("number", "numberOfElements", "totalElements", "totalPages", "size"):
        if type(payload.get(key)) is not int or payload[key] < 0:
            raise InvalidPage("Missing or invalid numeric pagination field: " + key)
    if type(payload.get("last")) is not bool:
        raise InvalidPage("Missing or invalid last flag")
    if payload["number"] != expected_page:
        raise InvalidPage("API returned the wrong page number")
    records = payload["content"]
    if not all(isinstance(record, dict) for record in records):
        raise InvalidPage("Each content entry must be an object")
    if payload["numberOfElements"] != len(records) or payload["size"] < len(records) or payload["size"] == 0:
        raise InvalidPage("Inconsistent page size or element count")
    total_pages = payload["totalPages"]
    total_records = payload["totalElements"]
    expected_pages = (total_records + payload["size"] - 1) // payload["size"]
    if total_pages != expected_pages and not (total_records == 0 and total_pages == 1):
        raise InvalidPage("Total page count is inconsistent with size and total elements")
    if expected_page >= max(total_pages, 1):
        raise InvalidPage("Page number exceeds total pages")
    if payload["last"] != (expected_page == max(total_pages, 1) - 1):
        raise InvalidPage("Last flag contradicts page count")
    if not payload["last"] and len(records) != payload["size"]:
        raise InvalidPage("Nonfinal page is incomplete")
    if previous_pages is not None and total_pages != previous_pages:
        raise InvalidPage("Page count changed; source snapshot may have changed")
    if previous_total is not None and total_records != previous_total:
        raise InvalidPage("Record count changed; source snapshot may have changed")
    if previous_size is not None and payload["size"] != previous_size:
        raise InvalidPage("Page size changed during extraction")
    return records


class APIClient:
    def __init__(self, config, pat, deadline, transport=None, clock=time.monotonic, sleep=time.sleep):
        if transport is None:
            import requests
            transport = requests.Session()
            transport.trust_env = False
        self.transport = transport
        self.config = config
        self.pat = pat
        self.deadline = deadline
        self.clock = clock
        self.sleep = sleep
        self.authorization = None

    def close(self):
        self.transport.close()

    def request_json(self, method, url, **kwargs):
        import requests
        for attempt in range(4):
            if self.deadline - self.clock() < 45:
                raise RetryableError("Invocation budget reached; retry from checkpoint")
            response = None
            retry_delay = min(2 ** attempt, 8)
            try:
                response = self.transport.request(method, url, timeout=(5, 20),
                                                  allow_redirects=False, stream=True, **kwargs)
                status = response.status_code
                if status == 429 or status in (500, 502, 503, 504):
                    retry_after = response.headers.get("Retry-After")
                    if retry_after:
                        try:
                            retry_delay = max(retry_delay, float(retry_after))
                        except ValueError:
                            from email.utils import parsedate_to_datetime
                            try:
                                retry_delay = max(retry_delay, (parsedate_to_datetime(retry_after)
                                                  - datetime.now(timezone.utc)).total_seconds())
                            except (TypeError, ValueError):
                                raise RetryableError("Unrecognized Retry-After; retry on the next invocation") from None
                elif not 200 <= status < 300:
                    if status in (401, 403):
                        raise FatalAPIError("Authentication rejected; verify or rotate the stored credential")
                    raise FatalAPIError("HTTP request rejected with status " + str(status))
                else:
                    body = bytearray()
                    for chunk in response.iter_content(chunk_size=65536):
                        if self.clock() >= self.deadline - 15:
                            raise RetryableError("Response download exceeded invocation budget")
                        body.extend(chunk)
                        if len(body) > self.config["max_response_bytes"]:
                            raise InvalidPage("Response exceeds size limit; reduce page size and start a new run")
                    try:
                        return json.loads(body)
                    except (ValueError, UnicodeError):
                        raise InvalidPage("API returned invalid JSON") from None
            except requests.RequestException:
                pass
            finally:
                if response is not None:
                    response.close()
            if attempt == 3 or self.clock() + retry_delay + 45 >= self.deadline:
                raise RetryableError("HTTP retries exhausted; retry on the next invocation", retry_after_seconds=retry_delay)
            self.sleep(retry_delay)
        raise RetryableError("HTTP retries exhausted")

    def authenticate(self):
        auth = self.config["auth"]
        if not self.pat or self.pat == "NOT_CONFIGURED":
            raise ConfigurationError("Provision the Snowflake PAT secret securely before running")
        if auth["mode"] == "basic":
            from requests.auth import HTTPBasicAuth
            self.transport.auth = HTTPBasicAuth(auth["username"], self.pat)
            self.authorization = {"Accept": "application/json"}
            return
        if auth["mode"] == "direct_pat":
            token = self.pat
        else:
            body = copy.deepcopy(auth["exchange_body"])
            headers = {"Accept": "application/json"}
            if auth.get("pat_json_field"):
                body[auth["pat_json_field"]] = self.pat
            if auth.get("pat_header"):
                headers[auth["pat_header"]] = auth.get("pat_prefix", "") + self.pat
            result = self.request_json("POST", auth["exchange_url"], json=body, headers=headers)
            token = result.get(auth["token_field"]) if isinstance(result, dict) else None
            if not isinstance(token, str) or not token:
                raise FatalAPIError("Token exchange did not return the configured token field")
        self.authorization = {"Accept": "application/json", auth["header"]: auth.get("prefix", "") + token}

    def fetch_page(self, page):
        if self.authorization is None:
            self.authenticate()
        request = self.config["request"]
        params = copy.deepcopy(request["params"])
        body = copy.deepcopy(request["json"])
        target = params if request["pagination_location"] == "params" else body
        target[request["page_parameter"]] = page
        target[request["size_parameter"]] = self.config["page_size"]
        kwargs = {"params": params, "headers": self.authorization}
        if request["method"] == "POST":
            kwargs["json"] = body
        return self.request_json(request["method"], request["url"], **kwargs)


class SnowflakeStore:
    def __init__(self, session):
        self.session = session

    def execute(self, sql, params=None):
        return self.session.sql(sql.replace("{schema}", TARGET_SCHEMA), params=params).collect()

    def acquire(self, owner):
        self.execute("UPDATE {schema}.INGEST_LOCK SET OWNER = ?, ACQUIRED_AT = CURRENT_TIMESTAMP() WHERE LOCK_ID = 1 AND OWNER IS NULL", [owner])
        rows = self.execute("SELECT OWNER FROM {schema}.INGEST_LOCK WHERE LOCK_ID = 1")
        return len(rows) == 1 and rows[0]["OWNER"] == owner

    def release(self, owner):
        self.execute("UPDATE {schema}.INGEST_LOCK SET OWNER = NULL, ACQUIRED_AT = NULL WHERE LOCK_ID = 1 AND OWNER = ?", [owner])

    def config(self):
        rows = self.execute("SELECT SETTINGS FROM {schema}.INGEST_CONFIG WHERE CONFIG_ID = 1")
        if len(rows) != 1:
            raise ConfigurationError("Expected exactly one configuration row")
        return decode(rows[0]["SETTINGS"])

    def open_run(self, config):
        rows = self.execute("SELECT *, IFF(NEXT_ATTEMPT_AT > CURRENT_TIMESTAMP(), TRUE, FALSE) AS WAIT_FOR_RETRY FROM {schema}.INGEST_RUNS WHERE STATUS IN ('RUNNING', 'RETRYABLE', 'FAILED') ORDER BY STARTED_AT DESC")
        if len(rows) > 1:
            raise ConfigurationError("Multiple unfinished runs; repair run state before continuing")
        if rows:
            run = rows[0].as_dict()
            if run["STATUS"] == "FAILED":
                raise ConfigurationError("Previous run failed validation; review and abandon it before restarting")
            if run["CONFIG_HASH"] != fingerprint(config):
                raise ConfigurationError("Configuration changed mid-run; restore it or abandon the unfinished run")
            if run.get("WAIT_FOR_RETRY"):
                return None
            self.execute("UPDATE {schema}.INGEST_RUNS SET STATUS = 'RUNNING', ERROR_CODE = NULL, NEXT_ATTEMPT_AT = NULL WHERE RUN_ID = ?", [run["RUN_ID"]])
            return run
        rows = self.execute("SELECT COUNT(*) AS RECENT FROM {schema}.INGEST_RUNS WHERE STATUS = 'COMPLETED' AND COMPLETED_AT > DATEADD('second', -?, CURRENT_TIMESTAMP())", [config["new_run_interval_seconds"]])
        if rows[0]["RECENT"]:
            return None
        run_id = str(uuid.uuid4())
        self.execute("INSERT INTO {schema}.INGEST_RUNS (RUN_ID, STATUS, CONFIG_HASH) VALUES (?, 'RUNNING', ?)", [run_id, fingerprint(config)])
        return {"RUN_ID": run_id, "NEXT_PAGE": 0, "EXPECTED_PAGES": None, "EXPECTED_RECORDS": None, "PAGE_SIZE": None, "RECORDS_LOADED": 0}

    def commit_page(self, run, payload):
        run_id = run["RUN_ID"]
        page = payload["number"]
        loaded = run["RECORDS_LOADED"] + len(payload["content"])
        if loaded > payload["totalElements"] or (payload["last"] and loaded != payload["totalElements"]):
            raise InvalidPage("Loaded record count does not match totalElements")
        status = "COMPLETED" if payload["last"] else "RUNNING"
        self.execute("BEGIN TRANSACTION")
        try:
            self.execute("DELETE FROM {schema}.RAW_RECORDS WHERE RUN_ID = ? AND PAGE_NUMBER = ?", [run_id, page])
            if payload["content"]:
                self.execute("INSERT INTO {schema}.RAW_RECORDS (RUN_ID, PAGE_NUMBER, RECORD_INDEX, PAYLOAD) SELECT ?, ?, entry.INDEX, entry.VALUE FROM TABLE(FLATTEN(INPUT => PARSE_JSON(?))) entry", [run_id, page, json.dumps(payload["content"])])
            self.execute("UPDATE {schema}.INGEST_RUNS SET NEXT_PAGE = ?, RECORDS_LOADED = ?, EXPECTED_PAGES = ?, EXPECTED_RECORDS = ?, PAGE_SIZE = ?, STATUS = ?, UPDATED_AT = CURRENT_TIMESTAMP(), COMPLETED_AT = IFF(? = 'COMPLETED', CURRENT_TIMESTAMP(), NULL), ERROR_CODE = NULL WHERE RUN_ID = ?", [page + 1, loaded, payload["totalPages"], payload["totalElements"], payload["size"], status, status, run_id])
            self.execute("COMMIT")
        except Exception:
            self.execute("ROLLBACK")
            raise
        run.update(NEXT_PAGE=page + 1, RECORDS_LOADED=loaded, EXPECTED_PAGES=payload["totalPages"], EXPECTED_RECORDS=payload["totalElements"], PAGE_SIZE=payload["size"], STATUS=status)

    def mark_error(self, run_id, status, code, retry_after_seconds=0):
        self.execute("UPDATE {schema}.INGEST_RUNS SET STATUS = ?, ERROR_CODE = ?, NEXT_ATTEMPT_AT = DATEADD('second', ?, CURRENT_TIMESTAMP()), UPDATED_AT = CURRENT_TIMESTAMP() WHERE RUN_ID = ?", [status, code, retry_after_seconds, run_id])


def process_pages(store, client, run, config, deadline, clock=time.monotonic):
    for unused in range(config["max_pages_per_invocation"]):
        if clock() >= deadline - 45:
            break
        payload = client.fetch_page(run["NEXT_PAGE"])
        validate_page(payload, run["NEXT_PAGE"], run["EXPECTED_PAGES"], run["EXPECTED_RECORDS"], run["PAGE_SIZE"])
        store.commit_page(run, payload)
        if payload["last"]:
            return {"status": "COMPLETED", "run_id": run["RUN_ID"], "records": run["RECORDS_LOADED"]}
    return {"status": "CHECKPOINTED", "run_id": run["RUN_ID"], "next_page": run["NEXT_PAGE"]}


def main(session):
    import _snowflake
    store = SnowflakeStore(session)
    owner = str(uuid.uuid4())
    if not store.acquire(owner):
        return {"status": "BUSY", "detail": "Another invocation owns the ingestion lock"}
    run = None
    client = None
    try:
        config = store.config()
        validate_config(config)
        run = store.open_run(config)
        if run is None:
            return {"status": "NOT_DUE"}
        deadline = time.monotonic() + config["invocation_budget_seconds"]
        client = APIClient(config, _snowflake.get_generic_secret_string("api_pat"), deadline)
        return process_pages(store, client, run, config, deadline)
    except Exception as error:
        safe_code = type(error).__name__ if isinstance(error, (ConfigurationError, InvalidPage, RetryableError, FatalAPIError)) else "INTERNAL_ERROR"
        if run is not None:
            status = "RETRYABLE" if isinstance(error, RetryableError) or safe_code == "INTERNAL_ERROR" else "FAILED"
            store.mark_error(run["RUN_ID"], status, safe_code, getattr(error, "retry_after_seconds", 0))
        raise RuntimeError("Ingestion stopped: " + safe_code + ". Review run state and configuration; no response or credentials logged.") from None
    finally:
        try:
            if client is not None:
                client.close()
        finally:
            store.release(owner)
