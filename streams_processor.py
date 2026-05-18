## faust command: faust -A streams_processor worker -l info

import ssl
import json
import joblib
import faust

from kafka_config import KafkaConfig

_model = joblib.load("model.pkl")
MODEL = _model["model"]
FEATURES = _model["features"]
print(f"  model loaded, expects features: {FEATURES}")

_cfg = KafkaConfig()
BROKER_URL = f"kafka://{_cfg.bootstrap_server}"

ssl_context = ssl.create_default_context()
broker_credentials = faust.SASLCredentials(
    username=_cfg.api_key,
    password=_cfg.secret_key,
    ssl_context=ssl_context,
)

app = faust.App(
    "streams-processor",  
    broker=BROKER_URL,
    broker_credentials=broker_credentials,
    value_serializer="raw", 
    store="memory://", 
)

raw_topic = app.topic("raw_data", value_serializer="raw")
predictions_topic = app.topic("predictions", value_serializer="raw")


def extract_features(row: dict) -> list[float] | None:
    values = []
    for col in FEATURES:
        v = row.get(col)
        if v is None or v == -200 or v == -200.0:
            return None  
        values.append(float(v))
    return values

def set_json (key, actual_co, predicted_class, status):
    return {
                "key": key,
                "actual_co": actual_co,
                "predicted_class": predicted_class,
                "status": status,
            }

@app.agent(raw_topic)
async def process(stream):
    async for raw_message in stream:
        row = json.loads(raw_message)

        key = f"{row.get('Date', '')}{row.get('Time', '')}"
        features = extract_features(row)

        if features is None:
            output = set_json(key, row.get("CO(GT)"), None, "skipped")
        else:
            output = set_json(key, row.get("CO(GT)"), str(MODEL.predict([features])[0]), "ok")

        await predictions_topic.send(
            key=key.encode("utf-8"),
            value=json.dumps(output).encode("utf-8"),
        )

        print(f"{key}  ||  {output['predicted_class']}  ({output['status']})")