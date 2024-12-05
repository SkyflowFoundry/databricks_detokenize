%sql
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