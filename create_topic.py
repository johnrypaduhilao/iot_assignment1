import kafka_config

config = kafka_config.KafkaConfig()

config.create_topic("raw_data")
config.create_topic("predictions")

