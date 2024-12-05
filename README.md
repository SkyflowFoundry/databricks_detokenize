# Skyflow for Databricks: Bulk Detokenize UDF
This repository contains a user-defined function that can be deployed within any Databricks instance in order to bulk detokenize Skyflow tokens in Databricks.

![databricks_dashboard](https://github.com/user-attachments/assets/f81227c5-fbbf-481c-b7dc-516f64ad6114)


Note: these examples are not an officially-supported product or recommended for production deployment without further review, testing, and hardening. Use with caution, this is sample code only.

## Create and register the Skyflow UDF
Start a new Notebook in Databricks, set the Notebook to SQL and copy/paste the below code after completing the TODOs.

### Prerequisites

**In Databricks**
- Configure and select the proper resource/cluster in Databricks that allows the execution of a python-wrapped SQL function.

**In Skyflow**
- Create or log into your account at skyflow.com and generate an API key: docs.skyflow.com
- Create a vault and relevant schema to hold your data
- Copy your API key, Vault URL, and Vault ID

#### SQL code
To use this code in your own account, complete the TODOs in the sample below:

```sql
CREATE OR REPLACE FUNCTION default.skyflow_bulk_detokenize(tokens ARRAY<STRING>, user_role STRING)
RETURNS ARRAY<STRING>
LANGUAGE PYTHON
AS $$
import requests
import math
from concurrent.futures import ThreadPoolExecutor, as_completed

SKYFLOW_API_URL = "-skyflow_api_endpoint-"
BEARER_TOKEN = "-skyflow_api_key-"

# Determine the redaction style based on the provided user info
if user_role == "admin":
    redaction_style = "PLAIN_TEXT"
elif user_role == "analyst":
    redaction_style = "PARTIAL"
else:
    redaction_style = "REDACTED"

def detokenize_chunk(chunk):
    """
    Function to detokenize a chunk of tokens.
    """
    headers = {
        "Authorization": f"Bearer {BEARER_TOKEN}",
        "Content-Type": "application/json"
    }

    payload = {
        "detokenizationParameters": [
            {"token": token, "redaction": redaction_style} for token in chunk
        ]
    }

    try:
        response = requests.post(SKYFLOW_API_URL, json=payload, headers=headers)
        response.raise_for_status()
        result = response.json()
        return [record["value"] for record in result["records"]]
    except requests.exceptions.RequestException as e:
        print(f"Error detokenizing chunk: {e}")
        return [f"Error: {str(e)}" for _ in chunk]

def bulk_detokenize(tokens):
    """
    Multi-threaded bulk detokenization.
    """
    if not tokens:
        return []

    MAX_TOKENS_PER_REQUEST = 25
    results = []

    # Split tokens into chunks
    chunks = [tokens[i:i + MAX_TOKENS_PER_REQUEST] for i in range(0, len(tokens), MAX_TOKENS_PER_REQUEST)]

    # Use ThreadPoolExecutor for multi-threading
    with ThreadPoolExecutor(max_workers = math.ceil(len(tokens) / MAX_TOKENS_PER_REQUEST)) as executor:
        # Submit all chunks to the executor
        future_to_chunk = {executor.submit(detokenize_chunk, chunk): chunk for chunk in chunks}

        for future in as_completed(future_to_chunk):
            results.extend(future.result())

    return results

return bulk_detokenize(tokens)
$$;
```

## Trigger the Skyflow UDF
Start a new Query or Dashboard in Databricks and copy/paste the below code after completing the TODOs.

### Prerequisites

- Insert a couple records in your Skyflow vault ensuring tokenization is enabled to receive back Skyflow tokens
- Insert the resulting Skyflow tokens in your Databricks instance

#### SQL code
To use this script set the user-role to match your tokenization style then run it. In the example below, the user-role is set to analyst.

```sql
USE hive_metastore.default;

WITH grouped_data AS (
    SELECT
        1 AS group_id,
        COLLECT_LIST(name) AS names,
        COLLECT_LIST(employee_id) AS employee_ids,
        COLLECT_LIST(role) AS roles,
        COLLECT_LIST(department) AS departments,
        COLLECT_LIST(date_joined) AS date_joineds,
        COLLECT_LIST(age) AS ages,
        COLLECT_LIST(business_unit) AS business_units,
        COLLECT_LIST(security_clearance) AS security_clearances
    FROM employee_data
    GROUP BY group_id
),
detokenized_batches AS (
    SELECT
        skyflow_bulk_detokenize(names, 'analyst') AS detokenized_names,
        employee_ids,
        roles,
        departments,
        date_joineds,
        ages,
        business_units,
        security_clearances
    FROM grouped_data
),
exploded_data AS (
    SELECT
        employee_ids[pos] AS employee_id,
        detokenized_names[pos] AS detokenized_name,
        roles[pos] AS role,
        departments[pos] AS department,
        date_joineds[pos] AS date_joined,
        ages[pos] AS age,
        business_units[pos] AS business_unit,
        security_clearances[pos] AS security_clearance
    FROM detokenized_batches
    LATERAL VIEW POSEXPLODE(detokenized_names) AS pos, detokenized_name
)
SELECT
    detokenized_name as name,
    role,
    department,
    date_joined,
    age,
    business_unit,
    security_clearance
FROM exploded_data;
```

# Learn more
To learn more about Skyflow Detokenization APIs visit docs.skyflow.com.
