import json
import boto3
from datetime import datetime
from decimal import Decimal
import uuid

# AWS services
dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

# CONFIG (IMPORTANT - change if needed)
TABLE_NAME = 'iot_telemetry'
BUCKET_NAME = 'iot-archive-suhas'

def lambda_handler(event, context):
    try:
        print("Received event:", json.dumps(event))

        # 🔹 Extract data
        value = event.get('value')
        topic = event.get('topic', '')

        # 🔹 Validation
        if value is None or value > 1000:
            print("Invalid data received")
            return {"status": "invalid"}

        # 🔹 Convert to Decimal (for DynamoDB)
        value = Decimal(str(value))
        fahrenheit = (value * Decimal(9)/Decimal(5)) + Decimal(32)

        # 🔹 Extract device_id
        if '/' in topic:
            device_id = topic.split('/')[1]
        else:
            device_id = "unknown_device"

        # 🔹 Metadata
        timestamp = datetime.utcnow().isoformat()
        record_id = str(uuid.uuid4())

        # 🔹 Final item
        item = {
            "record_id": record_id,
            "device_id": device_id,
            "timestamp": timestamp,
            "temperature_c": value,
            "temperature_f": fahrenheit,
            "unit": "Celsius"
        }

        # 🔹 Store in DynamoDB
        table = dynamodb.Table(TABLE_NAME)
        table.put_item(Item=item)

        print("Stored in DynamoDB")

        # 🔹 Store in S3 (archive)
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=f"iot-data/{device_id}_{timestamp}.json",
            Body=json.dumps(item, default=str),
            ContentType='application/json'
        )

        print("Stored in S3")

        return {
            "status": "success",
            "record_id": record_id
        }

    except Exception as e:
        print("ERROR:", str(e))
        return {
            "status": "error",
            "message": str(e)
        }