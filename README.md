# pipeline_bubble

Demo pipeline that pulls paged data from a Bubble app API and lands it in BigQuery. Everything is env-driven; no secrets are committed.

## Flow
1. Receive Bubble app URL + token.
2. Fetch rows with query params (see `main.py` / `old_main.py`).
3. Normalize JSON into a dataframe.
4. Append into a BigQuery table via `google-cloud-bigquery`.

## Configure
Set env vars before running:
- `GCP_PROJECT` – target GCP project.
- `GOOGLE_APPLICATION_CREDENTIALS` or `SERVICE_ACCOUNT_FILE` – service account JSON.
- `BUBBLE_APP_URL` – Bubble API base URL.
- `BUBBLE_API_TOKEN` – Bubble API bearer token.
- Adjust `table_id` inside `main.py` as needed.

## Run locally
```bash
pip install -r requirements.txt
python main.py
```

## Notes
- Replace sample URLs/tokens with non-sensitive values.
- `old_main.py` shows an alternate handler for Pub/Sub style input; keep it for reference or delete if not needed.
- Add retries/backoff and schema enforcement before production use.
