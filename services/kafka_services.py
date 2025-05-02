from aiokafka import AIOKafkaProducer
import os
from dotenv import load_dotenv
from utils.log import setup_logger
import json
from schemas import KafkaMessageSchema, TagSchema, TimeSeriesSchema
from datetime import datetime

load_dotenv('.env', override=True)
logger = setup_logger(__name__)


class KafkaService:
    def __init__(self, kafka_broker:str):
        self.producer = AIOKafkaProducer(
            bootstrap_servers = kafka_broker
        )
        self.is_started = False

    async def start(self):
        if not self.is_started:
            await self.producer.start()
            self.is_started = True
            logger.success("Kafka producer started")

    async def send_message(self, topic:str, message):
        if not self.is_started:
            await self.start()
        
        # Check for any Pydantic model using hasattr
        if hasattr(message, 'dict') and callable(message.dict):
            # It's a Pydantic model - convert to dict
            message = message.dict()
        
        # Always send data as JSON regardless of type
        if isinstance(message, (dict, list)):
            # For dictionaries and lists, use standard JSON serialization
            message_bytes = json.dumps(message, default=str).encode('utf-8')
        else:
            # For other types, wrap in a dictionary
            message_bytes = json.dumps({"value": str(message)}, default=str).encode('utf-8')
        
        logger.success(f"Sending message to Kafka topic: {topic}")
        await self.producer.send(topic, message_bytes)
        await self.producer.flush()
    def create_kafka_message_from_node_data(self, node_data) -> KafkaMessageSchema:
        """
        Create a KafkaMessageSchema from node_data dictionary
        Handles the actual structure of node_data from OPC UA client
        """
        # Extract timestamp - preserve the original timestamp
        timestamp = node_data.get('timestamp')
        
        # Handle both datetime objects and string timestamps
        if isinstance(timestamp, str):
            try:
                # Try to parse if it's a string, but don't modify the original
                parsed_timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                # If parsing succeeded, use the parsed datetime
                timestamp = parsed_timestamp
            except Exception as e:
                # Log the issue but don't change the timestamp
                logger.warning(f"Failed to parse timestamp string '{timestamp}': {e}")
                # Only use current time if timestamp is completely missing
                if not timestamp:
                    timestamp = datetime.now()
        elif not isinstance(timestamp, datetime) and timestamp is None:
            # Only use current time if timestamp is missing
            timestamp = datetime.now()
            logger.warning("Missing timestamp, using current time as fallback")
            
        # Log to ensure consistency
        logger.debug(f"Using timestamp: {timestamp} for Kafka message")
            
        return KafkaMessageSchema(
            tag_name=node_data.get('tag_name', ''),
            tag_id=node_data.get('tag_id', 0),
            value=str(node_data.get('value', '')),
            unit_of_measure=node_data.get('unit_of_measure', ''),
            description=node_data.get('description', ''),
            timestamp=timestamp
        )
        
    async def send_node_data(self, topic: str, node_data: dict):
        """
        Format node_data into KafkaMessageSchema and send to Kafka
        """
        try:
            kafka_message = self.create_kafka_message_from_node_data(node_data)
            await self.send_message(topic, kafka_message)
            return True
        except Exception as e:
            logger.error(f"Error sending node data to Kafka: {e}")
            return False

    async def close(self):
        if self.is_started:
            await self.producer.stop()
            self.is_started = False

kafka_service = KafkaService(os.getenv('KAFKA_BROKER'))
        