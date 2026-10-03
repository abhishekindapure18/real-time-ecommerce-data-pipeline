import json

from kafka import KafkaConsumer


consumer = KafkaConsumer(
    "ecommerce-events",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="ecommerce-consumer-group",
    value_deserializer=lambda value: json.loads(
        value.decode("utf-8")
    )
)


print("Kafka Consumer started...")


try:

    for message in consumer:

        event = message.value

        print(
            f"Received | "
            f"User: {event['user_id']} | "
            f"Product: {event['product_name']} | "
            f"Event: {event['event_type']} | "
            f"Price: ₹{event['price']} | "
            f"Quantity: {event['quantity']}"
        )

except KeyboardInterrupt:

    print("\nConsumer stopped.")

finally:

    consumer.close()