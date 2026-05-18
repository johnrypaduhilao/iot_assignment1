import json
import time
import signal
import sys
import pandas as pd
from confluent_kafka import Producer
 
from kafka_config import KafkaConfig
 
 
CSV_PATH = "dataset/AirQualityUCI.csv"
TOPIC = "raw_data"
DELAY_SECONDS = 1.0

def load_rows(path: str) -> list[dict]:
    
    df = pd.read_csv(path, sep=";", decimal=",")
    df = df.loc[:, ~df.columns.str.startswith("Unnamed")]
 
    df = df.dropna(how="all")
 
    return df.to_dict(orient="records")

def callback_func(err, msg) -> None:
    if err is not None:
        print(f"  Producer Error: {err}")
 
 
def main() -> None:
    rows = load_rows(CSV_PATH)
    
    print(f"  {len(rows)} rows for sending...")
 
    config = KafkaConfig().set_config()
    producer = Producer(config)
    print(f"Connected. Sending to topic '{TOPIC}'...\n")

    def handle_sigint(_sig, _frame):
        print("\nStopped...")
        producer.flush(timeout=5)
        sys.exit(0)
    signal.signal(signal.SIGINT, handle_sigint)
 
    for i, row in enumerate(rows, start=1):
        key = f"{row.get('Date', '')}T{row.get('Time', '')}"
 
        payload = json.dumps(row, default=str)
 
        producer.produce(
            topic=TOPIC,
            key=key,
            value=payload,
            callback=callback_func,
        )
        producer.poll(0)
 
        print(f"  [{i:4d}] sent | key={key} | CO(GT)={row.get('CO(GT)')}")
        time.sleep(DELAY_SECONDS)
 
    producer.flush(timeout=10)
    print("Done.")
 
 
if __name__ == "__main__":
    main()