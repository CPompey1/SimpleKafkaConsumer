from kafka import KafkaConsumer
import json
import subprocess
def main():
    commands_map = {}
    with open('commands.json', 'r') as commands_file:
        # Load the JSON file containing commands
        commands_map: dict = json.load(commands_file)
   

    # --- Configuration ---
    KAFKA_BROKER = 'localhost:9092'  # Replace with public IP or domain
    TOPIC_NAME = 'commands'                      # Replace with your Kafka topic

    # --- Kafka Consumer Setup ---
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',              # Start from beginning if no offset saved
        enable_auto_commit=True,                   # Automatically commit offsets
        group_id='my-internet-consumer-group',     # Consumer group name
        value_deserializer=lambda m: m.decode('utf-8')  # Convert byte to string
    )

    print(f"🌐 Connected to Kafka broker at {KAFKA_BROKER}")
    print(f"🔍 Listening for messages on topic '{TOPIC_NAME}'...")

    # --- Message Consumption Loop ---
    for message in consumer:
        print(f"📥 Received Message: {message.value}")
        result = subprocess.run(
            ['bash', '-c', commands_map.get(message.value, "echo Command not found")],
            check=True,
            capture_output=True,
            text=True
        )
        print(f"Command Output: {result.stdout}")
        if result.stderr:
            print(f"Command Error: {result.stderr.strip()}")
    print("Listening for commands... Press Ctrl+C to exit.")

    print("Exiting consumer.")
    consumer.close()


    

if __name__ == "__main__":
    main()
