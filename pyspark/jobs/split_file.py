import argparse
import logging
import math
import os
import shutil

from pyspark.sql import SparkSession


logger = logging.getLogger(__name__)


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Split large Lumi ingestion files using PySpark"
    )

    parser.add_argument(
        "--input-file",
        required=True,
        help="Input file path inside the Airflow container",
    )

    parser.add_argument(
        "--file-type",
        required=True,
        choices=["JSON", "CSV"],
        help="Input file type",
    )

    parser.add_argument(
        "--execution-id",
        required=True,
        help="Unique ingestion execution ID",
    )

    parser.add_argument(
        "--output-dir",
        required=True,
        help="Directory where split files will be written",
    )

    parser.add_argument(
        "--records-per-file",
        type=int,
        default=10000,
        help="Maximum number of records per split file",
    )

    return parser.parse_args()


def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def validate_arguments(args):
    if args.records_per_file <= 0:
        raise ValueError(
            "records-per-file must be greater than zero"
        )

    if not os.path.isfile(args.input_file):
        raise FileNotFoundError(
            f"Input file does not exist: {args.input_file}"
        )


def create_spark_session():
    return (
        SparkSession.builder
        .appName("LumiFileSplitter")
        .getOrCreate()
    )


def calculate_partition_count(record_count, records_per_file):
    return max(
        1,
        math.ceil(record_count / records_per_file)
    )


def prepare_output_directory(output_dir):
    if os.path.exists(output_dir):
        logger.info(
            "Removing existing output directory: %s",
            output_dir,
        )
        shutil.rmtree(output_dir)

    os.makedirs(output_dir, exist_ok=True)


def split_json(spark, input_file, output_dir, records_per_file):
    logger.info(
        "Reading JSON file: %s",
        input_file,
    )

    dataframe = (
    spark.read
    .option("multiLine", "true")
    .json(input_file)
)


    record_count = dataframe.count()

    logger.info(
        "JSON record count=%s",
        record_count,
    )

    if record_count == 0:
        raise ValueError(
            "Input JSON file contains no records"
        )

    partition_count = calculate_partition_count(
        record_count,
        records_per_file,
    )

    logger.info(
        "Creating %s JSON partitions",
        partition_count,
    )

    dataframe = dataframe.repartition(
        partition_count
    )

    dataframe.write \
        .mode("overwrite") \
        .json(output_dir)

    logger.info(
        "JSON splitting completed | "
        "execution output=%s",
        output_dir,
    )


def split_csv(spark, input_file, output_dir, records_per_file):
    logger.info(
        "Reading CSV file: %s",
        input_file,
    )

    dataframe = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .csv(input_file)
    )

    record_count = dataframe.count()

    logger.info(
        "CSV record count=%s",
        record_count,
    )

    if record_count == 0:
        raise ValueError(
            "Input CSV file contains no records"
        )

    partition_count = calculate_partition_count(
        record_count,
        records_per_file,
    )

    logger.info(
        "Creating %s CSV partitions",
        partition_count,
    )

    dataframe = dataframe.repartition(
        partition_count
    )

    (
        dataframe.write
        .mode("overwrite")
        .option("header", "true")
        .csv(output_dir)
    )

    logger.info(
        "CSV splitting completed | "
        "execution output=%s",
        output_dir,
    )


def main():
    configure_logging()

    args = parse_arguments()

    logger.info(
        "LUMI PYSPARK FILE SPLITTER STARTED | "
        "execution_id=%s | "
        "file_type=%s | "
        "input_file=%s | "
        "records_per_file=%s",
        args.execution_id,
        args.file_type,
        args.input_file,
        args.records_per_file,
    )

    validate_arguments(args)

    spark = create_spark_session()

    try:
        prepare_output_directory(args.output_dir)

        if args.file_type == "JSON":
            split_json(
                spark,
                args.input_file,
                args.output_dir,
                args.records_per_file,
            )

        elif args.file_type == "CSV":
            split_csv(
                spark,
                args.input_file,
                args.output_dir,
                args.records_per_file,
            )

        logger.info(
            "LUMI PYSPARK FILE SPLITTER COMPLETED | "
            "execution_id=%s",
            args.execution_id,
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
