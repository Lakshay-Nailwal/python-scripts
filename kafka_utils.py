import os
import json
import logging
from confluent_kafka import Producer
from dotenv import load_dotenv

# Load environment variables
load_dotenv('config.env')

# Setup logging
logging.basicConfig(level=logging.INFO)

def get_kafka_config(env=None):
    """
    Get Kafka configuration for different environments.
    
    Args:
        env (str, optional): Environment name ('qa2', 'staging', 'prod'). 
                           If None, uses BOOTSTRAP_SERVERS from config.env
    
    Returns:
        dict: Kafka producer configuration
    """
    # Environment-specific broker mappings from environment variables
    env_broker_keys = {
        'qa2': 'KAFKA_QA2_BROKER',
        'staging': 'KAFKA_STAGING_BROKER',
        'prod': 'KAFKA_PROD_BROKER'
    }
    
    if env and env in env_broker_keys:
        # Get broker from environment variable
        env_key = env_broker_keys[env]
        bootstrap_servers = os.getenv(env_key, 'localhost:9092')
    else:
        # Fallback to default BOOTSTRAP_SERVERS environment variable
        bootstrap_servers = os.getenv('BOOTSTRAP_SERVERS', 'localhost:9092')
    
    return {
        'bootstrap.servers': bootstrap_servers,
    }

def delivery_report(err, msg):
    """
    Callback function for message delivery reports.
    
    Args:
        err: Error object if message delivery failed
        msg: Message object if delivery was successful
    """
    if err is not None:
        logging.error(f"Message delivery failed: {err}")
    else:
        logging.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def push_to_kafka(topic, data, key=None, env='qa2'):
    """
    Push data to Kafka topic.
    
    Args:
        topic (str): Kafka topic name
        data (dict or str): Data to send (will be JSON serialized if dict)
        key (str, optional): Message key for partitioning
        env (str, optional): Environment ('qa2', 'staging', 'prod')
    
    Returns:
        bool: True if message was queued successfully, False otherwise
    """
    try:
        # Create producer with environment-specific config
        producer = Producer(get_kafka_config(env))
        
        # Convert data to JSON string if it's a dictionary
        if isinstance(data, dict):
            message = json.dumps(data)
        else:
            message = str(data)
        
        # Produce message
        producer.produce(
            topic=topic,
            value=message,
            key=key,
            callback=delivery_report
        )
        
        # Wait for message to be delivered
        producer.flush()
        
        env_info = f" (env: {env})" if env else ""
        logging.info(f"Successfully pushed message to topic: {topic}{env_info}")
        logging.info(f"Message: {message}")
        return True
        
    except Exception as e:
        logging.error(f"Error pushing to Kafka topic {topic}: {e}")
        return False


# Example usage:
# if __name__ == "__main__":
#     # Test data
#     test_data = {
#         "user_id": 123,
#         "action": "login",
#         "timestamp": "2024-12-20T10:30:00Z"
#     }
    
#     # Push single message to different environments
#     push_to_kafka("test-topic", test_data, key="user_123", env="qa2")
#     push_to_kafka("test-topic", test_data, key="user_123", env="staging")
    
#     # Push batch messages
#     batch_data = [
#         {"event": "page_view", "page": "home"},
#         {"event": "click", "button": "signup"}
#     ]
#     push_batch_to_kafka("events-topic", batch_data, env="qa2")