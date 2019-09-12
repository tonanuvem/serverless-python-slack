from kafka import KafkaConsumer

# To consume latest messages and auto-commit offsets
broker = "kafka.kubeless:9092"
consumer = KafkaConsumer('urls', bootstrap_servers=[broker])
for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value))

# consume earliest available messages, don't commit offsets
#KafkaConsumer(auto_offset_reset='earliest', enable_auto_commit=False)

# consume json messages
#KafkaConsumer(value_deserializer=lambda m: json.loads(m.decode('ascii')))

# consume msgpack
#KafkaConsumer(value_deserializer=msgpack.unpackb)

# StopIteration if no message after 1sec
KafkaConsumer(consumer_timeout_ms=1000)
