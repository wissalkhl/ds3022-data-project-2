from typing import Dict, List
import os, json, requests, boto3
from botocore.config import Config
from prefect import flow, task, get_run_logger

# --------- Shared config ---------
AWS_REGION = "us-east-1"
AWS_CFG = Config(region_name=AWS_REGION, retries={"max_attempts": 10, "mode": "standard"})
sqs = boto3.client("sqs", config=AWS_CFG)

# ===== Populate =====
@task(name="populate_queue", retries=2, retry_delay_seconds=3)
def populate_queue(uva_id: str) -> str:
    """POST the scatter endpoint once, receive SQS URL, store it for later."""
    logger = get_run_logger()
    if not uva_id or not uva_id.strip():
        raise ValueError("UVA_ID is required.")
    url = f"https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/{uva_id}"
    logger.info(f"POST {url}")

    resp = requests.post(url, timeout=30)
    resp.raise_for_status()
    payload: Dict = resp.json()  

    sqs_url = payload.get("sqs_url")
    if not sqs_url:
        raise RuntimeError(f"No 'sqs_url' in response: {payload}")

    with open(".env.scatter.json", "w") as f:
        json.dump({"SQS_READ_URL": sqs_url, "hello": payload.get("hello")}, f, indent=2)
    logger.info("Saved SQS URL to .env.scatter.json")
    return sqs_url

@flow(name="quote_pipeline_populate")
def run_populate(uva_id: str):
    """Calls the API exactly once to populate the queue."""
    _ = populate_queue(uva_id)

# ===== Monitor & Receive & Parse & Delete & Persist =====
def _load_read_queue_url() -> str:
    with open(".env.scatter.json") as f:
        return json.load(f)["SQS_READ_URL"]

@task(name="queue_attributes", log_prints=True)
def queue_attributes(queue_url: str) -> Dict[str, int]:
    """Return precise counters from SQS for monitoring."""
    attrs = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=[
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
            "ApproximateNumberOfMessagesDelayed",
        ],
    )["Attributes"]
    return {
        "visible": int(attrs.get("ApproximateNumberOfMessages", 0)),
        "inflight": int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0)),
        "delayed": int(attrs.get("ApproximateNumberOfMessagesDelayed", 0)),
    }

@task(name="receive_parse_delete", log_prints=True)
def receive_parse_delete(queue_url: str, expected: int = 21, visibility_timeout: int = 60) -> Dict[str, str]:
    """
    Long-polls SQS, parses MessageAttributes['order_no']['StringValue'] and ['word']['StringValue'],
    deletes messages immediately, and returns a dict of order_no -> word (as strings).
    Handles empty polls gracefully.
    """
    logger = get_run_logger()
    pairs: Dict[int, str] = {}
    raw_messages: List[Dict] = []

    while len(pairs) < expected:
        counts = queue_attributes.submit(queue_url).result()
        logger.info(
            f"counts: visible={counts['visible']} inflight={counts['inflight']} delayed={counts['delayed']} "
            f"collected={len(pairs)}/{expected}"
        )

        resp = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=20,              # long polling
            VisibilityTimeout=visibility_timeout,
            MessageAttributeNames=["All"],
            AttributeNames=["All"],
        )
        msgs = resp.get("Messages", [])
        if not msgs:
            logger.info("no messages this poll; continuing")
            continue

        delete_entries = []
        for m in msgs:
            raw_messages.append({
                "message_id": m["MessageId"],
                "body": m.get("Body", ""),
                "attributes": m.get("MessageAttributes", {}),
            })
            attrs = m.get("MessageAttributes") or {}
            try:
                order_no = int(attrs["order_no"]["StringValue"])
                word = attrs["word"]["StringValue"]
            except Exception:
                logger.info(f"skipping malformed message {m.get('MessageId')}")
                continue

            pairs[order_no] = word
            delete_entries.append({"Id": m["MessageId"], "ReceiptHandle": m["ReceiptHandle"]})

        if delete_entries:
            del_resp = sqs.delete_message_batch(QueueUrl=queue_url, Entries=delete_entries)
            failed = del_resp.get("Failed", [])
            if failed:
                raise RuntimeError(f"Failed to delete some messages: {failed}")

        logger.info(f"received {len(msgs)}; unique pairs={len(pairs)}/{expected}")

    persist_artifacts.submit(raw_messages, pairs)
    return {str(k): v for k, v in pairs.items()}

@task(name="persist_artifacts", log_prints=True)
def persist_artifacts(raw_messages, pairs):
    # Save raw messages
    with open("messages.json", "w") as f:
        json.dump(raw_messages, f, indent=2)

    ordered_rows = [{"order_no": k, "word": v}
                    for k, v in sorted({int(k): v for k, v in pairs.items()}.items())]

    with open("parts.json", "w") as f:
        json.dump(ordered_rows, f, indent=2)

    import pandas as pd
    pd.DataFrame(ordered_rows).to_csv("parts.csv", index=False)

    get_run_logger().info("wrote messages.json, parts.json, parts.csv (via pandas)")

@flow(name="quote_pipeline_collect")
def run_collect(expected: int = 21):
    """Monitors queue, receives/parses/deletes all messages, persists results."""
    logger = get_run_logger()
    queue_url = _load_read_queue_url()
    logger.info(f"queue: {queue_url}")
    d = receive_parse_delete(queue_url, expected=expected)
    logger.info(f"collected {len(d)} ordered pairs")


@task(name="assemble_phrase", log_prints=True)
def assemble_phrase(parts_path: str = "parts.json") -> str:
    """
    Reads parts.json: [{order_no: int, word: str}, ...]
    Sorts by order_no and joins words into the final phrase.
    """
    logger = get_run_logger()
    with open(parts_path) as f:
        rows = json.load(f)
    # rows like: [{"order_no": 0, "word": "Hello"}, ...]
    words = [row["word"] for row in sorted(rows, key=lambda r: int(r["order_no"]))]
    phrase = " ".join(words).strip()
    logger.info(f"assembled phrase length: {len(phrase)}")
    return phrase

@task(name="submit_solution", retries=2, retry_delay_seconds=3, log_prints=True)
def submit_solution(uva_id: str, phrase: str, platform: str = "prefect") -> Dict:
    """
    Sends the solution to the queue as message attributes.
    Verifies HTTPStatusCode == 200.
    """
    logger = get_run_logger()
    url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"

    # MessageBody can be any string; attributes carry the grading fields
    resp = sqs.send_message(
        QueueUrl=url,
        MessageBody="dp2-answer",
        MessageAttributes={
            "uvaid": {"DataType": "String", "StringValue": uva_id},
            "phrase": {"DataType": "String", "StringValue": phrase},
            "platform": {"DataType": "String", "StringValue": platform},
        },
    )
    status = resp.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status != 200:
        raise RuntimeError(f"SQS send_message returned status {status}, response={resp}")
    logger.info(f"submission accepted (HTTP {status}); MessageId={resp.get('MessageId')}")
    return resp

@flow(name="quote_pipeline_submit")
def run_submit(uva_id: str):
    """
    Assembles the phrase and submits it to the queue
    with message attributes: UVAid, phrase, platform='prefect'.
    """
    logger = get_run_logger()
    if not uva_id or not uva_id.strip():
        raise ValueError("UVA_ID is required for submission.")
    phrase = assemble_phrase()
    logger.info(f"phrase preview: {phrase[:80]}{'…' if len(phrase)>80 else ''}")
    _ = submit_solution(uva_id, phrase, platform="prefect")


if __name__ == "__main__":
    mode = os.getenv("MODE", "collect").lower()  
    if mode == "populate":
        uva = os.getenv("UVA_ID", "").strip() or input("Enter UVA_ID: ").strip()
        run_populate(uva)
    elif mode == "collect":
        run_collect()
    elif mode == "submit":
        uva = os.getenv("UVA_ID", "").strip() or input("Enter UVA_ID: ").strip()
        run_submit(uva)

