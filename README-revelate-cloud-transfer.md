# Load Scheduled Revelate Exports into Snowflake

Use Revelate to export query results to Amazon S3, then load the files through a Snowflake external stage.

**Flow:** Revelate query -> cloud transfer -> S3 -> Snowflake stage -> table.

## Before You Start

You need a Revelate query product, permission to schedule queries and configure transfers, a private S3 bucket, and administrative access to AWS IAM and Snowflake. Replace all `<PLACEHOLDERS>` below. Authenticate Revelate API requests using your organization's approved credentials; never include secrets in this guide or saved request examples.

## 1. Prepare S3 Access

Create a dedicated prefix such as `s3://<BUCKET>/revelate/<PRODUCT_CODE>/`.

- Authorize Revelate's writer identity to upload to that prefix. Obtain its identity and exact permissions from your Revelate administrator.
- Create a separate IAM role for Snowflake with `s3:GetBucketLocation`, prefix-scoped `s3:ListBucket`, and `s3:GetObject`/`s3:GetObjectVersion` permissions.
- If using customer-managed KMS encryption, grant the writer appropriate encryption permissions and the Snowflake reader decryption permissions.

## 2. Configure Revelate Delivery

In your Revelate Cloud Transfer Swagger UI, create a destination:

```http
POST https://<REVELATE_HOST>/api/cloud-transfer/v1/transfer/configs
Content-Type: application/json
```

```json
{
  "cloudProvider": "AWS",
  "pathPrefix": "<BUCKET>/revelate/<PRODUCT_CODE>/",
  "productFilter": ["<PRODUCT_CODE>"]
}
```

Save the returned configuration `code`, then verify it:

```http
POST https://<REVELATE_HOST>/api/cloud-transfer/v1/transfer/configs/<CONFIG_CODE>/verify
```

A **204** response activates delivery of new entitled files. The Revelate prefix does not include `s3://`. Keep the product filter explicit to avoid transferring unrelated products.

## 3. Test One Query Export

Send a query-result transfer to the verified destination:

```http
POST https://<REVELATE_HOST>/api/query/v4/query/<PRODUCT_CODE>/transfer
Content-Type: application/json
```

```json
{
  "parameters": {},
  "q": [],
  "fields": ["<COLUMN_1>", "<COLUMN_2>"],
  "transferConfigCodes": ["<CONFIG_CODE>"],
  "fileFormat": "CSV"
}
```

Supply any parameters required by your product. Save the returned `executionCode`; **202 means accepted, not delivered**. Check query execution and Cloud Transfer status, then inspect the completed S3 file. Confirm column order, delimiter, header, quoting, nulls, and compression.

## 4. Create The Snowflake Stage

Run as an authorized role with integration and schema-creation privileges. Use an existing database, schema, and warehouse. The CSV settings below assume comma-separated, double-quoted fields and one header row per file; adjust them to the actual export.

```sql
CREATE STORAGE INTEGRATION REVELATE_S3_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = '<SNOWFLAKE_READ_ROLE_ARN>'
  STORAGE_ALLOWED_LOCATIONS = ('s3://<BUCKET>/revelate/<PRODUCT_CODE>/');

DESCRIBE INTEGRATION REVELATE_S3_INT;
```

In AWS, update the read role's trust policy to allow `sts:AssumeRole` from the returned `STORAGE_AWS_IAM_USER_ARN`, with a `sts:ExternalId` condition matching `STORAGE_AWS_EXTERNAL_ID`. Then create the file format and stage:

```sql
CREATE FILE FORMAT REVELATE_CSV
  TYPE = CSV
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  ESCAPE_UNENCLOSED_FIELD = NONE
  COMPRESSION = AUTO;

CREATE STAGE REVELATE_STAGE
  URL = 's3://<BUCKET>/revelate/<PRODUCT_CODE>/'
  STORAGE_INTEGRATION = REVELATE_S3_INT
  FILE_FORMAT = REVELATE_CSV;

LIST @REVELATE_STAGE;
```

This path uses a **storage integration**, not an external access integration or a Revelate PAT stored in Snowflake.

## 5. Load And Validate

Create `<TARGET_TABLE>` with columns matching the exported CSV order and types. Use the path of a completed export:

```sql
COPY INTO <TARGET_TABLE>
  FROM @REVELATE_STAGE/<COMPLETED_EXPORT_PATH>/
  PATTERN = '.*[.]csv([.]gz)?'
  VALIDATION_MODE = RETURN_ALL_ERRORS;

COPY INTO <TARGET_TABLE>
  FROM @REVELATE_STAGE/<COMPLETED_EXPORT_PATH>/
  PATTERN = '.*[.]csv([.]gz)?'
  ON_ERROR = ABORT_STATEMENT
  PURGE = FALSE;
```

Reconcile loaded rows with the export's expected count. Keep unique immutable filenames and record export IDs. For full snapshots, publish only after every file is loaded and validated; do not append repeated snapshots into a current-state table or erase the previous result before its replacement succeeds.

## 6. Schedule And Monitor

Create a native Revelate query schedule, for example daily at 02:00 UTC:

```http
POST https://<REVELATE_HOST>/api/query/v4/schedules
Content-Type: application/json
```

```json
{
  "productCode": "<PRODUCT_CODE>",
  "attributes": {
    "fields": ["<COLUMN_1>", "<COLUMN_2>"],
    "parameters": {},
    "predicates": []
  },
  "cron": "0 2 * * *",
  "timezone": "UTC"
}
```

Save the returned schedule `code` and verify `nextRun`. Scheduled parameter values use objects such as `{"value":"example"}`, unlike the plain string values in one-time transfer requests.

**Confirm the schedule's delivery behavior with Revelate:** its request schema does not expose destination or file-format fields. If native scheduling does not deliver to your intended configuration, use an external scheduler to call the explicit transfer endpoint from step 3 instead. Do not enable both for the same extraction.

Schedule Snowflake `COPY INTO` with a task for batch loading, or use Snowpipe auto-ingest for file-arrival loading. A stage alone does not load data. For multipart snapshots, use a manifest or equivalent completion check rather than a fixed delay after the export schedule.

Monitor both services:

- Query executions: `GET /api/query/v4/executions?type=SCHEDULED&page=0&limit=20`.
- Transfer status: `GET /api/cloud-transfer/v1/transfer/summary?productCode=<PRODUCT_CODE>&page=0&size=20`.
- Snowflake: task/load failures, freshness, and row-count reconciliation.

## References

- Revelate Query Schedule: `https://<REVELATE_HOST>/api/query/swagger-ui/index.html#/Query%20Schedule`
- Revelate Cloud Transfer: `https://<REVELATE_HOST>/api/cloud-transfer/swagger-ui/index.html`
- [Snowflake S3 integration setup](https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration)
- [Snowflake COPY INTO](https://docs.snowflake.com/en/sql-reference/sql/copy-into-table)

Examples are templates. Confirm API availability and export behavior in your Revelate deployment before enabling recurring jobs.
