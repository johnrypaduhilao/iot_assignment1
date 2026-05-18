# Assignment Number 1 

**VIDEO LINK: https://drive.google.com/file/d/1yyL0P1z9ia26ApcB4eXm3KHg-LSfnScX/view?usp=drive_link**

Real-Time Streaming with Apache Kafka Assignment for ENGR 5785G

---

## Table of Contents
- [Datasets](#datasets)
- [Setup](#setup)
- [Architecture](#architecture)
- [Run Instructions](#run-instructions)
- [Streams Library](#streams-library)
- [Model Performance](#model-performance)
- [Notes](#notes)

---

## Datasets
B - Air Quality (UCI) - archive.ics.uci.edu/dataset/360 - Predict CO concentration .

---

## Setup
- In confluent.cloud create an account and a new cluster (Ex. iot_assignment_1).
- Once cluster is created, go to API Keys > Add Key. Grab the API Key and API Secret
- Go to Overview and grab the Bootstrap server.
- Create a .env file following the .env.example format with the values we mentioned above.
- Make sure you are able to run pipenv and git command in your terminal.
- git clone https://github.com/johnrypaduhilao/iot_assignment1.
- cd your-project
- Run command: pipenv install to install all required packages

---

## Architecture
- **Producer** reads the CSV one row at a time and publishes each row as
  a JSON message to the `raw_data` topic (one row per second).
- **Streams Processor** is a Faust agent subscribed to `raw_data`. It loads
  the trained model once at startup, predicts the CO class for each incoming
  row, and publishes the result to the `predictions` topic.
- **Output Consumer** subscribes to `predictions` and prints each result
  to the console with the actual value vs. the predicted class side-by-side.

The trained model (`model.pkl`) is produced offline by `model_training.py`
and read by the streams processor at startup.

---

## Run Instructions
- **Run Only Once:**  
    - pipenv run python create_topic.py to create required confluent topics
    - pipenv run python model_training.py to train model.
- **Run Consumer:**  
    - pipenv run python output_consumer.py
- **Run Streams Processor:**  
    - pipenv run faust -A streams_processor worker -l info
- **Run producer:**  
    - pipenv run python producer.py

---

## Streams Library
- **confluent-kafka** is used for the producer and output consumer.
  Both are simple with no stream processing logic making a low-level Kafka client the right tool. I chose this design as using a streams library here would be over-engineering the task without benefit.

- **faust-streaming** is used for the streams processor where most of the streaming logic lives.
  Faust's `@app.agent(raw_topic)` decorator provides a true Streams API:
  each message flows through an asynchronous generator (`async for msg in stream`),
  and Faust handles consumer group management, offset commits, and
  backpressure automatically.

---

## Model Performance
```
              precision    recall  f1-score   support

        High       1.00      1.00      1.00        23
         Low       0.96      0.94      0.95        77
      Medium       0.93      0.95      0.94        66

    accuracy                           0.95       166
   macro avg       0.96      0.96      0.96       166
weighted avg       0.95      0.95      0.95       166
```
---

## Notes
- **Dataset record cleanup.** The file uses semicolons as separators and European
  decimal notation (`2,6` instead of `2.6`). Both are handled by passing
  `sep=";"` and `decimal=","` to pandas and trimming trailing empty
  columns the CSV includes.

- **Missing sensor readings.** Sensor failures in the dataset are coded as
  `-200`. Rather than dropping these rows in the producer, they are sent
  through as-is so the processor can decide what to do which is a good exercise
  for handling real world sensor data.

- **Feature completeness vs. target completeness handling.**
  A row is marked `skipped` when one of the 12 input features is
  missing. If only the target `CO(GT)` is missing, the model still predicts 
  and the output shows `actual=MISSING, predicted=<class>`. 
  In real-world application, a sensor failure on the measurement device should not 
  prevent the model from doing its job.

- **Target binning for classification.** `CO(GT)` is a continuous value, but
  for me to get accuracy and F1 classification metrics, the target
  is separated into 3 different classes as a threshold: 
  Low (<2.0), Medium (2.0–4.0), and High (≥4.0). 