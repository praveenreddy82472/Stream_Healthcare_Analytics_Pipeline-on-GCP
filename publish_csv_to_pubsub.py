import csv
import json
from google.cloud import pubsub_v1, storage
import io

# Config variables
project_id = "project2-460014"
topic_id = "health-records-stream"
gcs_bucket_name = "streampro18"
gcs_file_name = "healthcare_dataset.csv"

# Initialize clients
publisher = pubsub_v1.PublisherClient()
storage_client = storage.Client()

topic_path = publisher.topic_path(project_id, topic_id)

def publish_csv_from_gcs():
    bucket = storage_client.bucket(gcs_bucket_name)
    blob = bucket.blob(gcs_file_name)

    # Download file content as bytes and decode to string
    content = blob.download_as_text()

    # Use csv.DictReader with StringIO to read CSV from string
    csv_file = io.StringIO(content)
    reader = csv.DictReader(csv_file)

    for row in reader:
        message_json = json.dumps(row)
        future = publisher.publish(topic_path, data=message_json.encode('utf-8'))
        print(f"Published message ID: {future.result()}")

if __name__ == "__main__":
    publish_csv_from_gcs()
