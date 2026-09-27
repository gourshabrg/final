"""Splits a large CSV/JSON file into part files; each record keeps its source_record_number."""
import argparse
import csv
import glob
import json
import logging
import math
import os
import shutil

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType

logger = logging.getLogger("lumi.pyspark.split")

FILE_TYPES = ("CSV", "JSON")
SOURCE_RECORD_NUMBER = "source_record_number"
# Holds the raw text of a record Spark could not read; Beam reports it as a parse error.
CORRUPT_RECORD = "_corrupt_record"
BYTE_ORDER_MARK = "﻿"


def parse_arguments(argv=None):
    parser = argparse.ArgumentParser(description="Split a Lumi ingestion file with PySpark")
    parser.add_argument("--input-file", required=True)
    parser.add_argument("--file-type", required=True, choices=FILE_TYPES)
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
    with open(path, encoding="utf-8-sig") as file:
        for line in file:
            if line.strip():
                return line.lstrip().startswith("[")
    return False


def split_file_count(record_count, records_per_file):
    return max(1, math.ceil(record_count / records_per_file))


def csv_header(path):
    with open(path, encoding="utf-8-sig", newline="") as file:
        return next(csv.reader(file), [])


def read_csv(spark, path) -> DataFrame:
    # All columns as text; _corrupt_record keeps rows with the wrong column count.
    schema = StructType([StructField(name, StringType()) for name in csv_header(path)]
                        + [StructField(CORRUPT_RECORD, StringType())])
    # Standard CSV: a quote inside a value is written "" and a quoted value may contain line breaks.
    return (spark.read.schema(schema).option("header", "true")
            .option("escape", '"').option("multiLine", "true")
            .option("mode", "PERMISSIVE").option("columnNameOfCorruptRecord", CORRUPT_RECORD)
            .csv(path))


def read_json_records(spark, path):
    """Raw JSON text per record; Spark's JSON reader is avoided because it changes field types."""
    if not is_json_array(path):
        lines = spark.sparkContext.textFile(path)
        return lines.map(lambda line: line.lstrip(BYTE_ORDER_MARK)).filter(lambda line: line.strip())
    with open(path, encoding="utf-8-sig") as file:
        try:
            records = json.load(file)
        except json.JSONDecodeError as error:
            raise ValueError(f"input is not valid JSON: {error.msg} at line {error.lineno}") from error
    return spark.sparkContext.parallelize([json.dumps(record) for record in records])


def number_json_record(text, number):
    try:
        record = json.loads(text)
    except ValueError:
        record = None
    if not isinstance(record, dict):
        record = {CORRUPT_RECORD: text}
    record[SOURCE_RECORD_NUMBER] = number
    return json.dumps(record)


def add_record_numbers(dataframe) -> DataFrame:
    # Number rows in file order before the shuffle; +1 so the first record is 1.
    schema = StructType(dataframe.schema.fields + [StructField(SOURCE_RECORD_NUMBER, LongType(), False)])
    numbered = dataframe.rdd.zipWithIndex().map(lambda pair: (*pair[0], pair[1] + 1))
    return dataframe.sparkSession.createDataFrame(numbered, schema)


def write_csv(dataframe, path):
    # Same quoting as the input, so Beam's CSV parser reads the values unchanged.
    dataframe.write.mode("overwrite").option("header", "true").option("escape", '"').csv(path)


def write_json_lines(spark, lines, path, file_count):
    spark.createDataFrame(lines.map(lambda line: (line,)), "value string") \
        .repartition(file_count).write.mode("overwrite").text(path)
    # Spark names text files part-*.txt; Beam reads part-*.json.
    for part in glob.glob(os.path.join(path, "part-*.txt")):
        os.rename(part, part[:-len(".txt")] + ".json")
    for checksum in glob.glob(os.path.join(path, ".*.crc")):
        os.remove(checksum)


def remove_old_output(args):
    # Remove files left by an earlier try of the same run.
    if os.path.exists(args.output_dir):
        logger.warning("execution_id=%s | removing old output in %s", args.execution_id, args.output_dir)
        shutil.rmtree(args.output_dir)


def split(spark, args):
    # Workers run the helper functions below, so they need this module too.
    spark.sparkContext.addPyFile(os.path.abspath(__file__))
    # Read once, used three times; Spark also needs it for _corrupt_record queries.
    if args.file_type == "CSV":
        records = read_csv(spark, args.input_file).cache()
    else:
        records = read_json_records(spark, args.input_file).cache()
    record_count = records.count()
    if record_count == 0:
        raise ValueError("Input file contains no records")

    file_count = split_file_count(record_count, args.records_per_file)
    logger.info("execution_id=%s | %s record(s) -> %s file(s)", args.execution_id, record_count, file_count)
    remove_old_output(args)

    # Number first: repartition shuffles the records, after that the original order is lost.
    if args.file_type == "CSV":
        write_csv(add_record_numbers(records).repartition(file_count), args.output_dir)
    else:
        numbered = records.zipWithIndex().map(lambda pair: number_json_record(pair[0], pair[1] + 1))
        write_json_lines(spark, numbered, args.output_dir, file_count)
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
