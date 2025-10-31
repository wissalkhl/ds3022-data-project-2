from datetime import datetime, timedelta
from typing import Dict, List

import os
import json
import requests
import boto3
from botocore.config import Config

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

AWS_REGION = "us-east-1"
AWS_CFG = Config(region_name=AWS_REGION, retries={"max_attempts": 10, "mode": "standard"})

def sqs_client():
    return boto3.client("sqs", config=AWS_CFG)

DATA_DIR = "/opt/airflow/data"
os.makedirs(DATA_DIR, exist_ok=True)

@dag(
    dag_id="quote_pipeline_airflow",
    start_date=datetime(2025, 1, 1),
    schedule=None,            # trigger manually
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=10)},
    params={"UVA_ID": ""},    # pass at trigger time
    tags=["dp2", "sqs", "airflow"],
)
def quote_pipeline_airflow():
    @task()
    def get_uva_id() -> str:
        ctx = get_current_context()
        u = (ctx["params"] or {}).get("UVA_ID") or os.environ.get("UVA_ID", "")
        u = u.strip()
        if not u:
            raise ValueError("Set UVA_ID via DAG params when triggering, or export UVA_ID in the Airflow environment.")
        print(f"Using UVA_ID={u}")
        return u

    @task()
    def populate_queue(uva_id: str) -> str:
        url = f"https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/{uva_id}"
        print(f"POST {url}")
        resp = requests.post(url, timeout=30)
        resp.raise_for_status()
        payload: Dict = resp.json()
        sqs_url = payload.get("sqs_url")
        if not sqs_url:
            raise RuntimeError(f"No 'sqs_url' in response: {payload}")
        with open(os.path.join(DATA_DIR, ".env.scatter.json"), "w") as f:
            json.dump({"SQS_READ_URL": sqs_url, "hello": payload.get("hello")}, f, indent=2)
        print(f"SQS queue URL: {sqs_url}")
        return sqs_url

    @task()
    def queue_attributes(queue_url: str) -> Dict[str, int]:
        s = sqs_client()
        attrs = s.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=[
                "ApproximateNumberOfMessages",
                "ApproximateNumberOfMessagesNotVisible",
                "ApproximateNumberOfMessagesDelayed",
            ],
        )["Attributes"]
        counts = {
            "visible": int(attrs.get("ApproximateNumberOfMessages", 0)),
            "inflight": int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0)),
            "delayed": int(attrs.get("ApproximateNumberOfMessagesDelayed", 0)),
        }
        print(f"counts: {counts}")
        return counts

    @task()
    def collect_messages(queue_url: str, expected: int = 21, visibility_timeout: int = 60) -> List[Dict]:
        s = sqs_client()
        pairs: Dict[int, str] = {}
        raw_messages: List[Dict] = []

        while len(pairs) < expected:
            _ = queue_attributes(queue_url)  # precise monitoring each loop

            resp = s.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20,           # long polling
                VisibilityTimeout=visibility_timeout,
                MessageAttributeNames=["All"],
                AttributeNames=["All"],
            )
            msgs = resp.get("Messages", [])
            if not msgs:
                print("no messages this poll; continuing")
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
                    print(f"skipping malformed message {m.get('MessageId')}")
                    continue

                pairs[order_no] = word
                delete_entries.append({"Id": m["MessageId"], "ReceiptHandle": m["ReceiptHandle"]})

            if delete_entries:
                del_resp = s.delete_message_batch(QueueUrl=queue_url, Entries=delete_entries)
                if del_resp.get("Failed"):
                    raise RuntimeError(f"Failed to delete some messages: {del_resp.get('Failed')}")
            print(f"received {len(msgs)}; unique pairs={len(pairs)}/{expected}")

        # persist artifacts (JSON; CSV optional)
        with open(os.path.join(DATA_DIR, "messages.json"), "w") as f:
            json.dump(raw_messages, f, indent=2)
        ordered_rows = [{"order_no": k, "word": v} for k, v in sorted(pairs.items())]
        with open(os.path.join(DATA_DIR, "parts.json"), "w") as f:
            json.dump(ordered_rows, f, indent=2)

        try:
            import pandas as pd
            pd.DataFrame(ordered_rows).to_csv(os.path.join(DATA_DIR, "parts.csv"), index=False)
        except Exception:
            pass  # CSV optional

        print("wrote messages.json, parts.json, parts.csv (if pandas available)")
        return ordered_rows  # XCom: [{order_no:int, word:str}, ...]

    @task()
    def assemble_phrase(ordered_rows: List[Dict]) -> str:
        words = [r["word"] for r in sorted(ordered_rows, key=lambda r: int(r["order_no"]))]
        phrase = " ".join(words).strip()
        print(f"assembled phrase length: {len(phrase)}")
        return phrase

    @task(retries=1, retry_delay=timedelta(seconds=5))
    def submit_solution(uva_id: str, phrase: str) -> Dict:
        s = sqs_client()
        url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
        resp = s.send_message(
            QueueUrl=url,
            MessageBody="dp2-answer",
            MessageAttributes={
                "uvaid":   {"DataType": "String", "StringValue": uva_id},
                "phrase":  {"DataType": "String", "StringValue": phrase},
                "platform":{"DataType": "String", "StringValue": "airflow"},
            },
        )
        status = (resp.get("ResponseMetadata") or {}).get("HTTPStatusCode")
        if status != 200:
            raise RuntimeError(f"SQS send_message returned status {status}, response={resp}")
        print(f"submission accepted (HTTP {status}); MessageId={resp.get('MessageId')}")
        return resp

    # wiring
    uva = get_uva_id()
    read_queue_url = populate_queue(uva)
    parts = collect_messages(read_queue_url)
    phrase = assemble_phrase(parts)
    _ = submit_solution(uva, phrase)

quote_pipeline_airflow()

