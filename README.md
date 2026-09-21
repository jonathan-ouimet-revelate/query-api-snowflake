# Revelate Query API to Snowflake Quickstart

This repository has a variety of Snowflake objects defined that allow for the repeated ingestion of data from a Revelate Query API product. It requires a Revelate user and personal access token that has an active order for a query view product. With that information added in Snowflake, and network access rules in place, Snowflake can reach out to the api for the product, authenticate, and pull the data in, in a paginated fashion.

### Before beginning to use this repository, please note that there are other options for getting data into Snowflake using Revelate. Another method for accomplishing the same thing is using Scheduled Query API extractions in Revelate, whereby files are generated and sent to a bucket endpoint. This assumes the consumer has access to a bucket that they can attach to their Snowflake instance as a `stage` and then read the generated files in to Snowflake in a similar fashion using a recurring task. This second option requires a slightly bit more configuration on the Revelate side, but slightly less on the Snowflake side, and does require access to a bucket for both read/write operations, preferably with the ability to manipulate bucket permissions.

## Files

| File | Purpose |
| --- | --- |
| `setup.sql` | Ready-to-open SQL worksheet: tables, views, network rule, placeholder secret, external access integration, embedded Python procedure, suspended task |
| `ingest.py` | Editable Python source embedded into setup.sql by the builder |
| `config.example.json` | Non-secret endpoint, authentication adapter, request, paging, and runtime configuration |
| `admin_prerequisites.sql` | Optional administrator grants, reviewed and run separately |
| `test_run.sql` | Starts one serverless task invocation, even while its schedule is suspended |
| `activate.sql` | Explicitly enables recurring execution |
| `operations.sql` | Read-only run status, task history, and query examples |
| `sample_response.json` | Local copy of the supplied Desktop/query.json response |
| `test_ingest.py` | Offline tests; uses simulated HTTP and database behavior |
| `setup.template.sql` | Worksheet template used by the builder |
| `build_bundle.py` | Regenerates setup.sql and optionally creates a ZIP |

## Setup Order

1. Confirm the API contract with its provider. The sample is a response, not a request body. Do not send sample_response.json as the query request.
2. For the verified endpoint, use `config.se.json`. For another API, edit `config.example.json` and leave `contract_confirmed` false until its request contract is verified.
3. Regenerate the working `setup.sql` by running `python3 build_bundle.py --config config.se.json` from this folder. The worksheet embeds the complete Python handler, so no stage upload or local Python connection to Snowflake is required for deployment.
4. Have an authorized administrator review `admin_prerequisites.sql` if SYSADMIN lacks those account privileges. `CREATE INTEGRATION` is broad; it is included to support SYSADMIN object ownership, not as a least-privilege production role design. All CREATE statements in setup.sql run as SYSADMIN. Confirm access to the selected database/schema if they already exist.
5. Open `setup.sql` in a Snowflake SQL worksheet. Select an existing small warehouse for setup queries. Run it in order, stopping immediately on errors. It creates `QUERY_API_STARTER.INGEST` and the account-level integration `QUERY_API_STARTER_EAI`. If those names already belong to another application, choose unused names before running. Change all references, including TARGET_SCHEMA in ingest.py, then regenerate. Do not blindly rerun the full worksheet after partial setup: CREATE intentionally fails rather than replacing existing data or resetting run state.
6. Provision the real PAT into the Snowflake GENERIC_STRING secret `QUERY_API_STARTER.INGEST.QUERY_API_PAT` through your organization's approved secret-management process. The worksheet creates only the inert marker `NOT_CONFIGURED`. Do not put credentials in JSON, Python, ZIP files, worksheet source, or chat. When working with Cortex Code, provide credentials through `/secrets` and request secure provisioning separately; a Cortex Code secret is NOT automatically the Snowflake runtime secret. This bundle does not provision real credentials.
7. If your account requires acceptance of third-party package terms, have an administrator enable the package source for `requests` before creating the procedure. Snowpark and requests are installed by Snowflake at runtime; `requirements.txt` is for offline local tests only.
8. Run `test_run.sql`. Execution is asynchronous. Use `operations.sql` to inspect task history and INGEST_RUNS. If the run is checkpointed, execute test_run.sql again after the first invocation finishes. Repeat until COMPLETED. A successful task invocation may mean CHECKPOINTED or NOT_DUE, not a complete extraction.
9. After verifying row counts, representative records, source consistency, and the schedule, run `activate.sql`. Nothing in setup.sql resumes the task.

## Configuration

This export uses the verified SE URL, Basic authentication username, page size 20, and an hourly suspended task. The runtime PAT is excluded: new deployments require secret provisioning. The generic config.example.json remains unconfigured.

| Setting | Meaning |
| --- | --- |
| `contract_confirmed` | Set true only after verifying the documented API contract |
| `allowed_hosts` | Explicit lowercase DNS hostnames for the query API and, if different, token-exchange API; used to generate the egress rule |
| `request.url` | HTTPS URL without credentials, embedded query parameters, fragments, or nonstandard ports |
| `request.method` | GET or POST |
| `request.params` | Non-secret URL query parameters, such as filters and stable sorting |
| `request.json` | Non-secret POST JSON body with the actual query/filter specification |
| `request.pagination_location` | `params` or `json`; JSON pagination requires POST |
| `request.page_parameter` | Top-level request field for the zero-based page number; example `page` is an assumption |
| `request.size_parameter` | Top-level request field for requested page size; example `size` is an assumption |
| `post_is_read_only` | Must be true for POST query requests so retries do not repeat writes |
| `page_size` | Request size, initially 100; use the provider's supported maximum that fits safely in memory |
| `max_pages_per_invocation` | Checkpoint boundary, initially 100; NOT a total page limit |
| `invocation_budget_seconds` | Soft HTTP-processing budget, initially 200 seconds; accepted range 60-240 |
| `max_response_bytes` | Per-page decompressed response limit, initially 8 MiB |
| `new_run_interval_seconds` | Minimum cooldown after a completed snapshot before starting the next, initially 3600 |

The hourly task schedule is a low-frequency starting point, not a known business requirement. Large extractions continue on subsequent scheduled invocations; at an hourly schedule, each continuation can wait an hour. Shorten the task interval if needed, accepting the extra compute for NOT_DUE checks. The snapshot cooldown is measured from completion, so this is NOT an exact top-of-hour snapshot schedule. Setting a daily interval instead reduces polling costs when daily refresh is sufficient. For massive/slow APIs, benchmark an external worker rather than assuming Snowflake is the cheapest runtime.

If changing configuration after setup, suspend the task and wait for any active invocation to finish. Update the single INGEST_CONFIG.SETTINGS value using PARSE_JSON and the non-secret configuration. Regenerating a local worksheet does not update Snowflake automatically. If hosts change, also update the network rule. Configuration is fingerprinted: changing it mid-extraction blocks continuation until you restore it or explicitly abandon that extraction.

## Authentication Adapters

`basic` uses HTTP Basic authentication with `auth.username` and the PAT from the Snowflake runtime secret as its password. Credentials are handled by requests.auth.HTTPBasicAuth, not stored in configuration or logged. The SE endpoint configuration is in `config.se.json`.

`direct_pat` sends the PAT in the configured header on each data request. The default header/prefix are `Authorization` and `Bearer `; verify both. A provider may instead require an `X-API-Key` header with an empty prefix.

`exchange` makes a POST to `auth.exchange_url` once per invocation. It sends the PAT in a configurable top-level JSON field (`pat_json_field`) and/or header (`pat_header`, optionally with `pat_prefix`), along with `exchange_body`. It reads the token from the configured top-level `token_field`, then uses `header` and `prefix` on page requests. The example field names `pat` and `access_token` are assumptions, not a verified contract. Both hosts must be in allowed_hosts.

The exchange adapter supports JSON only. Form-encoded credentials, nested token fields, cookies, asynchronous query-job creation, and dynamically generated query IDs require adapter changes once documentation is available. Exchange POST retries assume token issuance can safely be retried. Redirects are rejected to avoid forwarding credentials to unintended endpoints. A 401/403 stops the extraction; the starter does not repeatedly attempt authentication or automatically refresh an expired token within one invocation.

## Tables And Views

- `RAW_RECORDS`: one row per object in content, with RUN_ID, PAGE_NUMBER, RECORD_INDEX, original PAYLOAD as VARIANT, and INGESTED_AT. This includes completed and partial/abandoned runs.
- `INGEST_RUNS`: expected counts, committed counts, next page, retry time, status, and timestamps. No credentials or raw API errors are stored here.
- `INGEST_CONFIG`: one non-secret configuration document. Do not grant untrusted roles write access: configuration controls where the runtime credential is sent within the permitted hosts.
- `INGEST_LOCK`: singleton ownership record that serializes invocations. Do not insert additional lock/config rows.
- `HISTORY_RECORDS`: records from all COMPLETED snapshots only.
- `LATEST_RECORDS`: records from the most recently COMPLETED snapshot. A valid empty snapshot correctly produces an empty view rather than exposing stale records.

No natural key is assumed. `polygon_id` has not been proven unique across layers or across the entire dataset. All source records, including identical source objects, are retained. Geometry strings remain unchanged in VARIANT; no CRS or validity assumptions are imposed.

## Pagination And Completeness

The response contract matches the supplied sample: content, number, numberOfElements, size, totalElements, totalPages, and last. Page numbering starts at zero. The handler rejects wrong/repeated page numbers, inconsistent metadata, changed totals/page size, invalid JSON, oversized pages, and final count mismatches. An API which intentionally omits totals or returns partial nonfinal pages needs a different validator.

Records for a page and its checkpoint are committed in one database transaction. A rollback leaves both unchanged. Resume uses the next committed page. There is no fixed maximum total number of pages: the per-invocation cap and runtime budget yield a checkpoint and later invocations continue that extraction. Each page must still fit the response-size and transaction limits. The 300-second task timeout is the hard fallback; the Python time budget is not a hard limit on database calls or runtime startup.

IMPORTANT: Metadata/count checks cannot detect all duplicates or omissions caused by source changes. The supplied response is unsorted. Require a deterministic unique sort AND a stable query snapshot/query ID, or a source dataset that does not change during the entire extraction. A stable sort alone is not sufficient when records are inserted/deleted between requests. Include a provider-issued snapshot/query ID in the configured request if its API supports that. Obtaining and renewing such IDs is provider-specific and is not implemented. Snapshot expiry during a long resumed extraction requires abandoning and restarting it.

## Retry And Recovery

HTTP timeouts, 429, and selected 5xx responses receive up to four attempts per request, with bounded exponential waits. Numeric or HTTP-date Retry-After is honored; if it exceeds the current budget, the next eligible retry time is persisted. Authentication errors are not retried. Bodies, headers, credentials, and arbitrary exception messages are not written to application logs. Access controls and retention still apply to raw records and query history because ingestion SQL carries the data into Snowflake.

A transient failure marks the run RETRYABLE and fails the task invocation. The next invocation resumes it after NEXT_ATTEMPT_AT. Three consecutive task failures auto-suspend the task, requiring an operator to investigate and resume it. A validation/authentication failure marks the run FAILED and blocks new snapshots so it cannot be silently skipped. Review the provider contract, source consistency, configuration, and secret validity. The last successful view remains available.

For recovery, suspend the task first and verify no invocation is still running (suspending a task does not cancel its active execution). For manual CALLs, also check those sessions. Use reviewed, targeted statements rather than resetting everything:

```sql
USE ROLE SYSADMIN;
ALTER TASK QUERY_API_STARTER.INGEST.QUERY_API_POLL SUSPEND;
```

After an authentication-only fix, retry the same stable snapshot with the same request configuration by setting the specific failed run to RETRYABLE:

```sql
UPDATE QUERY_API_STARTER.INGEST.INGEST_RUNS
SET STATUS = 'RETRYABLE', ERROR_CODE = NULL, NEXT_ATTEMPT_AT = NULL
WHERE RUN_ID = '<reviewed-run-id>' AND STATUS = 'FAILED';
```

For a changed/expired snapshot or changed configuration, abandon the specific unfinished run instead:

```sql
UPDATE QUERY_API_STARTER.INGEST.INGEST_RUNS
SET STATUS = 'ABANDONED', UPDATED_AT = CURRENT_TIMESTAMP()
WHERE RUN_ID = '<reviewed-run-id>' AND STATUS IN ('RUNNING', 'RETRYABLE', 'FAILED');
```

A hard task cancellation can leave the singleton lock held because Python finally blocks are not guaranteed to run. There is deliberately no automatic time-based lock stealing: an old but active invocation must never race a new writer. ONLY after confirming its owning invocation has terminated, clear the exact inspected owner:

```sql
UPDATE QUERY_API_STARTER.INGEST.INGEST_LOCK
SET OWNER = NULL, ACQUIRED_AT = NULL
WHERE LOCK_ID = 1 AND OWNER = '<inspected-lock-owner>';
```

Run test_run.sql and inspect completion before resuming the schedule. Avoid invoking this procedure inside an explicit caller transaction. Task execution uses the default autocommit context needed for the lock and scoped page transactions. Do not directly modify RAW_RECORDS or checkpoint values while ingestion is running.

Historical and abandoned rows are retained indefinitely by design. Agree on a retention policy before production: recurring full snapshots grow storage. This bundle does not delete data automatically. Setup grants and SYSADMIN ownership are for the requested starter; a production deployment should use a reviewed least-privilege owner role, restricted configuration access, credential rotation, and failure notifications.

## Local Verification And Packaging

Tests require Python 3.10+ and requests. They do not contact any API or Snowflake account. From the extracted folder:

```bash
python3 -m pip install -r requirements.txt
python3 test_ingest.py
python3 build_bundle.py --config config.se.json
python3 build_bundle.py --config config.se.json --output "$HOME/Desktop/query-api-working-new.zip"
```

The builder includes only explicitly named bundle files, refuses to overwrite an existing ZIP, and checks ZIP integrity. It does not scan arbitrary request settings for accidentally inserted secrets: NEVER add credentials to the project before packaging.

The offline suite covers direct/exchange authentication shapes, response validation, pagination beyond an invocation cap, resume after failure, bounded retries, Retry-After, redirection blocking, malformed/oversized responses, transaction rollback simulation, and incomplete-run isolation. The worksheet embeds the same ingest.py source.

## References

- https://docs.snowflake.com/en/developer-guide/external-network-access/creating-using-external-network-access
- https://docs.snowflake.com/en/user-guide/tasks-intro
- https://docs.snowflake.com/en/user-guide/tasks-python-jvm
- https://docs.snowflake.com/en/sql-reference/transactions
