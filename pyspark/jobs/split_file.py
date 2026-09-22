"""
Splits a large CSV or JSON file into smaller files so Beam can read them in parallel (Phase 2).

Output: <output-dir>/part-*.csv or part-*.json (JSON is written one object per line).
"""
import argparse
import logging
import math
import os
import shutil

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger("lumi.pyspark.split")


def parse_arguments(argv=None):
    parser = argparse.ArgumentParser(description="Split a Lumi ingestion file with PySpark")
    parser.add_argument("--input-file", required=True)
    parser.add_argument("--file-type", required=True, choices=["CSV", "JSON"])
    parser.add_argument("--execution-id", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--records-per-file", type=int, default=10000)
    args = parser.parse_args(argv)

    if args.records_per_file <= 0:
        parser.error("--records-per-file must be greater than zero")
    if not os.path.isfile(args.input_file):
        parser.error(f"input file does not exist: {args.input_file}")
    return args


def is_json_array(path):
    # A JSON array must be read in multiLine mode; one-object-per-line JSON must not.
    with open(path, encoding="utf-8-sig") as file:
        for line in file:
            if line.strip():
                return line.lstrip().startswith("[")
    return False


def split_file_count(record_count, records_per_file):
    return max(1, math.ceil(record_count / records_per_file))


def read_input(spark, path, file_type) -> DataFrame:
    if file_type == "CSV":
        # Keep every column as text; Beam does the type checks.
        return spark.read.option("header", "true").option("inferSchema", "false").csv(path)
    return spark.read.option("multiLine", str(is_json_array(path)).lower()).json(path)


def write_output(dataframe, path, file_type):
    writer = dataframe.write.mode("overwrite")
    if file_type == "CSV":
        writer.option("header", "true").csv(path)
    else:
        writer.json(path)


def split(spark, args):
    dataframe = read_input(spark, args.input_file, args.file_type)
    record_count = dataframe.count()
    if record_count == 0:
        raise ValueError("Input file contains no records")

    file_count = split_file_count(record_count, args.records_per_file)
    logger.info("execution_id=%s | %s record(s) -> %s file(s)", args.execution_id, record_count, file_count)

    # Remove files left by an earlier try of the same run.
    if os.path.exists(args.output_dir):
        logger.warning("execution_id=%s | removing old output in %s", args.execution_id, args.output_dir)
        shutil.rmtree(args.output_dir)
    write_output(dataframe.repartition(file_count), args.output_dir, args.file_type)
    return file_count


def main(argv=None):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [pyspark-split] %(message)s")
    args = parse_arguments(argv)
    logger.info("execution_id=%s | start input=%s type=%s records_per_file=%s",
                args.execution_id, args.input_file, args.file_type, args.records_per_file)

    spark = SparkSession.builder.appName(f"lumi-split-{args.execution_id}").getOrCreate()
    try:
        split(spark, args)
        logger.info("execution_id=%s | split files written to %s", args.execution_id, args.output_dir)
    except Exception:
        logger.exception("execution_id=%s | split failed", args.execution_id)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
