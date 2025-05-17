import argparse
import time
import logging
import json
import typing
from datetime import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions, PipelineOptions, StandardOptions
from apache_beam.transforms.combiners import CountCombineFn

# Define schema with NamedTuple
class HealthRecord(typing.NamedTuple):
    Name: str
    Age: int
    Gender: str
    BloodType: str
    MedicalCondition: str
    DateofAdmission: str
    Doctor: str
    Hospital: str
    InsuranceProvider: str
    BillingAmount: float
    RoomNumber: int
    AdmissionType: str
    DischargeDate: str
    Medication: str
    TestResults: str
    processing_timestamp: str

beam.coders.registry.register_coder(HealthRecord, beam.coders.RowCoder)

def parse_json(element):
    try:
        row = json.loads(element.decode('utf-8'))
        return HealthRecord(
            Name=row.get("Name", ""),
            Age=int(row.get("Age", 0)),
            Gender=row.get("Gender", ""),
            BloodType=row.get("BloodType", ""),
            MedicalCondition=row.get("MedicalCondition", ""),
            DateofAdmission=row.get("DateofAdmission", "1970-01-01"),
            Doctor=row.get("Doctor", ""),
            Hospital=row.get("Hospital", ""),
            InsuranceProvider=row.get("InsuranceProvider", ""),
            BillingAmount=float(row.get("BillingAmount", 0.0)),
            RoomNumber=int(row.get("RoomNumber", 0)),
            AdmissionType=row.get("AdmissionType", ""),
            DischargeDate=row.get("DischargeDate", "1970-01-01"),
            Medication=row.get("Medication", ""),
            TestResults=row.get("TestResults", ""),
            processing_timestamp=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        )
    except Exception as e:
        logging.error(f"Failed to parse JSON: {element}, error: {e}")
        return None

def add_processing_timestamp(element):
    row = element._asdict()
    row['processing_timestamp'] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    return row

class ComputeAggregates(beam.DoFn):
    def process(self, records, window=beam.DoFn.WindowParam):
        records = list(records)
        if not records:
            return

        billing_amounts = [r.BillingAmount for r in records]
        window_start = window.start.to_utc_datetime().strftime("%Y-%m-%dT%H:%M:%S")

        yield {
            'timestamp': window_start,
            'record_count': len(records),
            'avg_billing_amount': sum(billing_amounts) / len(billing_amounts),
            'max_billing_amount': max(billing_amounts)
        }

def run():
    parser = argparse.ArgumentParser(description='Stream JSON from Pub/Sub to BigQuery')
    parser.add_argument('--project', required=True)
    parser.add_argument('--region', required=True)
    parser.add_argument('--staging_location', required=True)
    parser.add_argument('--temp_location', required=True)
    parser.add_argument('--runner', required=True)
    parser.add_argument('--input_topic', required=True)
    parser.add_argument('--agg_table_name', required=True)
    parser.add_argument('--raw_table_name', required=True)
    parser.add_argument('--window_duration', required=True)

    opts, _ = parser.parse_known_args()

    options = PipelineOptions(
        streaming=True,
        save_main_session=True,
        flags=[],
        **{
            "worker_machine_type": "n1-standard-2",
            "max_num_workers": 2,
            "autoscaling_algorithm": "THROUGHPUT_BASED"
        }
    )

    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = opts.project
    google_cloud_options.region = opts.region
    google_cloud_options.staging_location = opts.staging_location
    google_cloud_options.temp_location = opts.temp_location
    google_cloud_options.job_name = 'streaming-health-pipeline-{}'.format(time.time_ns())

    standard_options = options.view_as(StandardOptions)
    standard_options.runner = opts.runner

    window_duration = int(opts.window_duration)

    raw_table_schema = {
        "fields": [
            {"name": "Name", "type": "STRING"},
            {"name": "Age", "type": "INTEGER"},
            {"name": "Gender", "type": "STRING"},
            {"name": "BloodType", "type": "STRING"},
            {"name": "MedicalCondition", "type": "STRING"},
            {"name": "DateofAdmission", "type": "DATE"},
            {"name": "Doctor", "type": "STRING"},
            {"name": "Hospital", "type": "STRING"},
            {"name": "InsuranceProvider", "type": "STRING"},
            {"name": "BillingAmount", "type": "FLOAT"},
            {"name": "RoomNumber", "type": "INTEGER"},
            {"name": "AdmissionType", "type": "STRING"},
            {"name": "DischargeDate", "type": "DATE"},
            {"name": "Medication", "type": "STRING"},
            {"name": "TestResults", "type": "STRING"},
            {"name": "processing_timestamp", "type": "STRING"}
        ]
    }

    agg_table_schema = {
        "fields": [
            {"name": "timestamp", "type": "STRING"},
            {"name": "record_count", "type": "INTEGER"},
            {"name": "avg_billing_amount", "type": "FLOAT"},
            {"name": "max_billing_amount", "type": "FLOAT"}
        ]
    }

    p = beam.Pipeline(options=options)

    parsed_msgs = (
        p
        | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(topic=opts.input_topic)
        | 'ParseJson' >> beam.Map(parse_json).with_output_types(typing.Optional[HealthRecord])
        | 'FilterNone' >> beam.Filter(lambda x: x is not None)
    )

    (
        parsed_msgs
        | 'AddTimestamp' >> beam.Map(add_processing_timestamp)
        | 'WriteRawToBQ' >> beam.io.WriteToBigQuery(
            opts.raw_table_name,
            schema=raw_table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
    )

    (
        parsed_msgs
        | 'WindowInto' >> beam.WindowInto(beam.window.FixedWindows(window_duration))
        | 'GroupIntoList' >> beam.transforms.util.BatchElements()
        | 'ComputeAggregates' >> beam.ParDo(ComputeAggregates())
        | 'WriteAggToBQ' >> beam.io.WriteToBigQuery(
            opts.agg_table_name,
            schema=agg_table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
    )

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Pipeline is starting...")
    p.run().wait_until_finish()

if __name__ == '__main__':
    run()
