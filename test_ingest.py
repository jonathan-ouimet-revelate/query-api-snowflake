import copy
import json
import unittest
from pathlib import Path
from unittest.mock import patch

import requests

from ingest import (
    APIClient, ConfigurationError, FatalAPIError, InvalidPage, RetryableError,
    SnowflakeStore, fingerprint, process_pages, validate_config, validate_page,
)


ROOT = Path(__file__).resolve().parent


def config():
    settings = json.loads((ROOT / "config.example.json").read_text())
    settings["contract_confirmed"] = True
    settings["allowed_hosts"] = ["api.unit.test"]
    settings["request"]["url"] = "https://api.unit.test/query"
    settings["auth"]["exchange_url"] = "https://api.unit.test/token"
    return settings


def page(number=0, total=5, size=2):
    pages = (total + size - 1) // size
    records = [{"id": index} for index in range(number * size, min((number + 1) * size, total))]
    return {"content": records, "number": number, "size": size,
            "numberOfElements": len(records), "totalElements": total,
            "totalPages": pages, "last": number == max(pages, 1) - 1}


def run_state():
    return {"RUN_ID": "test-run", "NEXT_PAGE": 0, "EXPECTED_PAGES": None,
            "EXPECTED_RECORDS": None, "PAGE_SIZE": None, "RECORDS_LOADED": 0}


class Response:
    def __init__(self, payload=None, status=200, headers=None, raw=None):
        self.status_code = status
        self.headers = headers or {}
        self.body = raw if raw is not None else json.dumps(payload).encode()
        self.closed = False

    def iter_content(self, chunk_size):
        yield self.body

    def close(self):
        self.closed = True


class Transport:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        result = self.responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    def close(self):
        pass


class MemoryStore:
    def __init__(self):
        self.records = {}

    def commit_page(self, run, payload):
        loaded = run["RECORDS_LOADED"] + len(payload["content"])
        if loaded > payload["totalElements"] or (payload["last"] and loaded != payload["totalElements"]):
            raise InvalidPage("Record count mismatch")
        self.records[(run["RUN_ID"], payload["number"])] = copy.deepcopy(payload["content"])
        run.update(NEXT_PAGE=payload["number"] + 1, RECORDS_LOADED=loaded,
                   EXPECTED_PAGES=payload["totalPages"], EXPECTED_RECORDS=payload["totalElements"],
                   PAGE_SIZE=payload["size"], STATUS="COMPLETED" if payload["last"] else "RUNNING")


class PageClient:
    def __init__(self, total=5, fail_at=None):
        self.total = total
        self.fail_at = fail_at
        self.requested = []

    def fetch_page(self, number):
        self.requested.append(number)
        if number == self.fail_at:
            raise RetryableError("Temporary failure")
        return page(number, total=self.total)


class FakeRow(dict):
    def as_dict(self):
        return dict(self)


class FakeSession:
    def __init__(self, fail_update=False):
        self.commands = []
        self.records = ["previous-complete-row"]
        self.checkpoint = 0
        self.fail_update = fail_update
        self.snapshot = None

    def sql(self, statement, params=None):
        self.commands.append((statement, params))
        if statement == "BEGIN TRANSACTION":
            self.snapshot = (copy.deepcopy(self.records), self.checkpoint)
        elif statement.startswith("INSERT INTO"):
            self.records.extend(json.loads(params[2]))
        elif statement.startswith("UPDATE"):
            if self.fail_update:
                raise RuntimeError("Simulated checkpoint write failure")
            self.checkpoint = params[0]
        elif statement == "ROLLBACK":
            self.records, self.checkpoint = self.snapshot
        return self

    def collect(self):
        return []


class ConfigurationTests(unittest.TestCase):
    def test_unconfigured_defaults_fail_closed(self):
        with self.assertRaises(ConfigurationError):
            validate_config(json.loads((ROOT / "config.example.json").read_text()))

    def test_direct_pat_configuration(self):
        validate_config(config())

    def test_reject_http_and_unapproved_host(self):
        for endpoint in ("http://api.unit.test/query", "https://other.unit.test/query", "https://user:pass@api.unit.test/query", "https://api.unit.test/query?token=bad"):
            with self.subTest(endpoint=endpoint):
                settings = config()
                settings["request"]["url"] = endpoint
                with self.assertRaises(ConfigurationError):
                    validate_config(settings)

    def test_post_requires_read_only_confirmation(self):
        settings = config()
        settings["request"]["method"] = "POST"
        with self.assertRaises(ConfigurationError):
            validate_config(settings)
        settings["post_is_read_only"] = True
        validate_config(settings)

    def test_stable_config_hash(self):
        self.assertEqual(fingerprint({"first": 1, "second": 2}), fingerprint({"second": 2, "first": 1}))


class PaginationTests(unittest.TestCase):
    def test_supplied_sample(self):
        payload = json.loads((ROOT / "sample_response.json").read_text())
        self.assertEqual(len(validate_page(payload, 0)), 5)
        self.assertEqual(payload["totalElements"], 34)

    def test_empty_result(self):
        self.assertEqual(validate_page(page(total=0), 0), [])

    def test_repeated_page_rejected(self):
        with self.assertRaises(InvalidPage):
            validate_page(page(0), 1)

    def test_wrong_last_flag_rejected(self):
        payload = page(0)
        payload["last"] = True
        with self.assertRaises(InvalidPage):
            validate_page(payload, 0)

    def test_changed_count_rejected(self):
        with self.assertRaises(InvalidPage):
            validate_page(page(1), 1, previous_pages=3, previous_total=6)

    def test_missing_fields_rejected(self):
        with self.assertRaises(InvalidPage):
            validate_page({"content": []}, 0)

    def test_wrong_content_type_rejected(self):
        payload = page(0)
        payload["content"] = ["not an object", {}]
        with self.assertRaises(InvalidPage):
            validate_page(payload, 0)

    def test_no_fixed_total_page_limit(self):
        store = MemoryStore()
        run = run_state()
        settings = config()
        settings["max_pages_per_invocation"] = 2
        client = PageClient(total=15)
        while run.get("STATUS") != "COMPLETED":
            process_pages(store, client, run, settings, 200, clock=lambda: 0)
        self.assertEqual(client.requested, list(range(8)))
        self.assertEqual(run["RECORDS_LOADED"], 15)

    def test_failure_resumes_without_reloading_committed_pages(self):
        store = MemoryStore()
        run = run_state()
        with self.assertRaises(RetryableError):
            process_pages(store, PageClient(fail_at=1), run, config(), 200, clock=lambda: 0)
        self.assertEqual(run["NEXT_PAGE"], 1)
        client = PageClient()
        result = process_pages(store, client, run, config(), 200, clock=lambda: 0)
        self.assertEqual(client.requested, [1, 2])
        self.assertEqual(result["status"], "COMPLETED")
        self.assertEqual(result["records"], 5)

    def test_deadline_yields_checkpoint_not_completion(self):
        result = process_pages(MemoryStore(), PageClient(), run_state(), config(), 40, clock=lambda: 0)
        self.assertEqual(result["status"], "CHECKPOINTED")
        self.assertEqual(result["next_page"], 0)


class HTTPTests(unittest.TestCase):
    def client(self, responses, settings=None):
        transport = Transport(responses)
        client = APIClient(settings or config(), "test-only-dummy-value", 200, transport=transport,
                           clock=lambda: 0, sleep=lambda duration: None)
        return client, transport

    def test_pat_header_and_get_pagination(self):
        client, transport = self.client([Response(page(0))])
        client.fetch_page(0)
        request = transport.calls[0][2]
        self.assertEqual(request["headers"]["Authorization"], "Bearer test-only-dummy-value")
        self.assertEqual(request["params"], {"page": 0, "size": 100})
        self.assertFalse(request["allow_redirects"])

    def test_basic_authentication_uses_requests_auth(self):
        settings = config()
        settings["auth"] = {"mode": "basic", "username": "test-user"}
        validate_config(settings)
        client, transport = self.client([Response(page(0))], settings)
        client.fetch_page(0)
        self.assertEqual(transport.auth.username, "test-user")
        self.assertEqual(transport.auth.password, "test-only-dummy-value")
        self.assertEqual(transport.calls[0][2]["headers"], {"Accept": "application/json"})

    def test_basic_username_required(self):
        settings = config()
        settings["auth"] = {"mode": "basic", "username": ""}
        with self.assertRaises(ConfigurationError):
            validate_config(settings)

    def test_exchange_then_post_json(self):
        settings = config()
        settings["auth"]["mode"] = "exchange"
        settings["request"]["method"] = "POST"
        settings["request"]["pagination_location"] = "json"
        settings["request"]["json"] = {"query": "sample"}
        client, transport = self.client([Response({"access_token": "dummy-access"}), Response(page(0))], settings)
        client.fetch_page(0)
        self.assertEqual(transport.calls[0][2]["json"]["pat"], "test-only-dummy-value")
        self.assertEqual(transport.calls[1][2]["headers"]["Authorization"], "Bearer dummy-access")
        self.assertEqual(transport.calls[1][2]["json"], {"query": "sample", "page": 0, "size": 100})
        self.assertEqual(settings["request"]["json"], {"query": "sample"})

    def test_rate_limit_and_server_error_retried(self):
        client, transport = self.client([Response(status=429), Response(status=503), Response(page(0))])
        self.assertEqual(client.fetch_page(0)["number"], 0)
        self.assertEqual(len(transport.calls), 3)

    def test_network_timeout_retried(self):
        client, transport = self.client([requests.Timeout("dummy"), Response(page(0))])
        client.fetch_page(0)
        self.assertEqual(len(transport.calls), 2)

    def test_long_retry_after_yields_without_early_retry(self):
        client, transport = self.client([Response(status=429, headers={"Retry-After": "900"})])
        with self.assertRaises(RetryableError):
            client.fetch_page(0)
        self.assertEqual(len(transport.calls), 1)

    def test_bounded_retries(self):
        client, transport = self.client([Response(status=503) for unused in range(4)])
        with self.assertRaises(RetryableError):
            client.fetch_page(0)
        self.assertEqual(len(transport.calls), 4)

    def test_auth_rejection_does_not_leak_body(self):
        client, transport = self.client([Response({"secret": "DO_NOT_LOG"}, status=401)])
        with self.assertRaises(FatalAPIError) as caught:
            client.fetch_page(0)
        self.assertNotIn("DO_NOT_LOG", str(caught.exception))
        self.assertEqual(len(transport.calls), 1)

    def test_redirect_rejected(self):
        client, transport = self.client([Response(status=302)])
        with self.assertRaises(FatalAPIError):
            client.fetch_page(0)
        self.assertEqual(len(transport.calls), 1)

    def test_response_size_limit(self):
        settings = config()
        settings["max_response_bytes"] = 2
        client, unused = self.client([Response(page())], settings)
        with self.assertRaises(InvalidPage):
            client.fetch_page(0)

    def test_non_json_response(self):
        client, unused = self.client([Response(raw=b"not JSON")])
        with self.assertRaises(InvalidPage):
            client.fetch_page(0)

    def test_placeholder_secret_rejected(self):
        client, transport = self.client([])
        client.pat = "NOT_CONFIGURED"
        with self.assertRaises(ConfigurationError):
            client.fetch_page(0)
        self.assertEqual(transport.calls, [])


class StorageTests(unittest.TestCase):
    def test_page_and_checkpoint_rollback_together(self):
        session = FakeSession(fail_update=True)
        run = run_state()
        with self.assertRaises(RuntimeError):
            SnowflakeStore(session).commit_page(run, page(0))
        self.assertEqual(session.records, ["previous-complete-row"])
        self.assertEqual(session.checkpoint, 0)
        self.assertEqual(run["NEXT_PAGE"], 0)
        self.assertEqual(session.commands[-1][0], "ROLLBACK")

    def test_page_checkpoint_commit(self):
        session = FakeSession()
        run = run_state()
        SnowflakeStore(session).commit_page(run, page(0))
        self.assertEqual(session.checkpoint, 1)
        self.assertEqual(run["NEXT_PAGE"], 1)
        self.assertEqual(session.commands[-1][0], "COMMIT")

    def test_final_count_mismatch_not_published(self):
        session = FakeSession()
        run = run_state()
        run.update(NEXT_PAGE=2, RECORDS_LOADED=3)
        with self.assertRaises(InvalidPage):
            SnowflakeStore(session).commit_page(run, page(2))
        self.assertEqual(session.commands, [])

    def test_config_change_blocks_resume(self):
        store = SnowflakeStore(None)
        row = FakeRow(run_state(), STATUS="RUNNING", CONFIG_HASH="old")
        with patch.object(store, "execute", return_value=[row]):
            with self.assertRaises(ConfigurationError):
                store.open_run(config())

    def test_failed_run_blocks_new_snapshot(self):
        store = SnowflakeStore(None)
        row = FakeRow(run_state(), STATUS="FAILED", CONFIG_HASH=fingerprint(config()))
        with patch.object(store, "execute", return_value=[row]):
            with self.assertRaises(ConfigurationError):
                store.open_run(config())

    def test_retry_after_blocks_early_next_invocation(self):
        store = SnowflakeStore(None)
        row = FakeRow(run_state(), STATUS="RETRYABLE", CONFIG_HASH=fingerprint(config()), WAIT_FOR_RETRY=True)
        with patch.object(store, "execute", return_value=[row]) as execute:
            self.assertIsNone(store.open_run(config()))
            self.assertEqual(execute.call_count, 1)

    def test_error_persists_retry_delay(self):
        store = SnowflakeStore(None)
        with patch.object(store, "execute") as execute:
            store.mark_error("test-run", "RETRYABLE", "RetryableError", 900)
            self.assertEqual(execute.call_args.args[1], ["RETRYABLE", "RetryableError", 900, "test-run"])

    def test_lock_contention(self):
        store = SnowflakeStore(None)
        with patch.object(store, "execute", side_effect=[[], [FakeRow(OWNER="another-owner")]]):
            self.assertFalse(store.acquire("this-owner"))

    def test_views_filter_completed_runs_including_empty_latest(self):
        template = (ROOT / "setup.template.sql").read_text()
        self.assertIn("WHERE runs.STATUS = 'COMPLETED'", template)
        self.assertIn("FROM INGEST_RUNS\n    WHERE STATUS = 'COMPLETED'", template)
        self.assertNotIn("RESUME", template)


if __name__ == "__main__":
    unittest.main(verbosity=2)
