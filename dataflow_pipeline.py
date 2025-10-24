import json
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition

class ParseAndValidate(beam.DoFn):
    def process(self, element):
        try:
            record = json.loads(element.decode("utf-8"))
            if record.get("total_amount", 0) > 0:
                yield record
        except Exception as e:
            print(f"Malformed message: {e}")

class AddEnrichment(beam.DoFn):
    def process(self, record):
        product_map = {"P1001": "Milk", "P1002": "Bread", "P1003": "Eggs", "P1004": "Soap"}
        for item in record.get("items", []):
            item["product_name"] = product_map.get(item["product_id"], "Unknown")
        yield record

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--input_topic", required=True)
    parser.add_argument("--output_dataset", required=True)
    parser.add_argument("--runner", default="DirectRunner")
    parser.add_argument("--temp_location", default="")
    parser.add_argument("--staging_location", default="")
    known_args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args)
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        messages = (p
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(topic=known_args.input_topic, with_attributes=False)
            | "ParseJSON" >> beam.ParDo(ParseAndValidate())
            # | "Deduplicate" >> beam.RemoveDuplicates(lambda r: r["event_id"])
            | "Deduplicate" >> beam.Distinct(key=lambda r: r["event_id"])
            | "EnrichData" >> beam.ParDo(AddEnrichment())
        )

        # Write to BigQuery (raw table)
        (messages
            | "WriteToBigQuery" >> WriteToBigQuery(
                table=f"{known_args.project}:{known_args.output_dataset}.raw_transactions",
                schema="SCHEMA_AUTODETECT",
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == "__main__":
    run()
