import boto3
import json
import logging
import urllib.parse

s3 = boto3.client("s3")
smr = boto3.client("sagemaker-runtime")

SOURCE_BUCKET = "reddit-crawler-bucket"
DEST_BUCKET = "x-enriched-bucket"
ENDPOINT_NAME = "huggingface-pytorch-inference-2025-08-31-17-46-16-561"
MAX_CHARS = 1024

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def _pick_best_label(result_json):
    if isinstance(result_json, list) and result_json:
        first = result_json[0]
        preds = first if isinstance(first, list) else result_json
        preds = [p for p in preds if isinstance(p, dict) and "label" in p]
        if preds:
            best = max(preds, key=lambda x: x.get("score", 0.0))
            return best.get("label", "unknown"), float(best.get("score", 0.0))
    return "unknown", 0.0

def lambda_handler(event, context):
    processed, failed = [], []

    logger.info(f"Received event: {json.dumps(event)}")

    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])  # decode URL-encoded key

        logger.info(f"Processing file: s3://{bucket}/{key}")

        if not key.endswith("/data.json"):
            logger.info(f"Skipping {key} because it does not end with /data.json")
            continue

        parts = key.split("/")
        if len(parts) != 4 or parts[0] != "tweets":
            logger.info(f"Skipping {key} because path structure is unexpected")
            continue

        ticker = parts[1].rsplit(".", 1)[0]
        date = parts[2].split("=", 1)[-1]
        dest_key = f"{ticker}-{date}.json"

        try:
            obj_data = s3.get_object(Bucket=bucket, Key=key)
            tweets = json.loads(obj_data["Body"].read().decode("utf-8"))
            logger.info(f"Loaded {len(tweets)} tweets from {key}")

            for i, tw in enumerate(tweets, start=1):
                text = (
                    tw.get("content")
                    or tw.get("text")
                    or tw.get("rawContent")
                    or tw.get("full_text")
                    or ""
                ).strip()

                if not text:
                    tw["sentiment"] = "unknown"
                    tw["sentiment_score"] = 0.0
                    continue

                text = text[:MAX_CHARS]

                try:
                    resp = smr.invoke_endpoint(
                        EndpointName=ENDPOINT_NAME,
                        ContentType="application/json",
                        Body=json.dumps({"inputs": text})
                    )
                    result = json.loads(resp["Body"].read().decode("utf-8"))
                    label, score = _pick_best_label(result)
                    tw["sentiment"] = label
                    tw["sentiment_score"] = score
                except Exception as model_err:
                    logger.error(f"Failed to run SageMaker model on tweet {i}: {model_err}")
                    tw["sentiment"] = "unknown"
                    tw["sentiment_score"] = 0.0

            logger.info(f"Writing enriched tweets to s3://{DEST_BUCKET}/{dest_key}")
            s3.put_object(
                Bucket=DEST_BUCKET,
                Key=dest_key,
                Body=json.dumps(tweets, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
                ContentType="application/json"
            )

            logger.info(f"Deleting original file s3://{bucket}/{key}")
            s3.delete_object(Bucket=bucket, Key=key)
            processed.append(key)

        except Exception as e:
            logger.error(f"Failed processing {key}: {str(e)}")
            failed.append({"file": key, "error": str(e)})

    logger.info(f"Processed files: {processed}")
    logger.info(f"Failed files: {failed}")
    return {"processed": processed, "failed": failed}