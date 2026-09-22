"""
Tests for the PySpark split job. Run inside the scheduler container (it has Spark and Java):
    docker exec lumi-airflow-scheduler python -m unittest discover -s /opt/lumi/pyspark/tests -v
"""
import glob
import json
import os
import sys
import tempfile
import unittest
from types import SimpleNamespace

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "jobs"))

import split_file  # noqa: E402

HEADER = "employee_id,first_name\n"


def read_lines(pattern):
    lines = []
    for part in glob.glob(pattern):
        with open(part, encoding="utf-8") as file:
            lines.extend(file.read().splitlines())
    return lines


def write(folder, name, text):
    path = os.path.join(folder, name)
    with open(path, "w", encoding="utf-8") as file:
        file.write(text)
    return path


class HelperTest(unittest.TestCase):

    def setUp(self):
        self.folder = tempfile.mkdtemp()

    def test_detects_json_array(self):
        self.assertTrue(split_file.is_json_array(write(self.folder, "a.json", '\n  [{"a": 1}]')))

    def test_detects_json_lines(self):
        self.assertFalse(split_file.is_json_array(write(self.folder, "b.json", '{"a": 1}\n{"a": 2}\n')))

    def test_file_count_rounds_up(self):
        self.assertEqual(3, split_file.split_file_count(101, 50))
        self.assertEqual(1, split_file.split_file_count(1, 50))

    def test_rejects_missing_input_file(self):
        with self.assertRaises(SystemExit):
            split_file.parse_arguments(["--input-file", "/nope.csv", "--file-type", "CSV",
                                        "--execution-id", "x", "--output-dir", self.folder])

    def test_rejects_zero_records_per_file(self):
        path = write(self.folder, "c.csv", HEADER)
        with self.assertRaises(SystemExit):
            split_file.parse_arguments(["--input-file", path, "--file-type", "CSV", "--execution-id", "x",
                                        "--output-dir", self.folder, "--records-per-file", "0"])


class SparkSplitTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.spark = SparkSession.builder.master("local[1]").appName("lumi-split-test").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.folder = tempfile.mkdtemp()

    def args(self, input_file, file_type, per_file):
        return SimpleNamespace(input_file=input_file, file_type=file_type, execution_id="test",
                               output_dir=os.path.join(self.folder, "out"), records_per_file=per_file)

    def test_csv_is_split_into_files_with_headers(self):
        rows = "".join(f"E{i:06d},Name{i}\n" for i in range(10))
        args = self.args(write(self.folder, "in.csv", HEADER + rows), "CSV", 4)

        self.assertEqual(3, split_file.split(self.spark, args))

        parts = glob.glob(os.path.join(args.output_dir, "*.csv"))
        self.assertEqual(3, len(parts))
        lines = read_lines(os.path.join(args.output_dir, "*.csv"))
        self.assertEqual(3, lines.count("employee_id,first_name"))
        self.assertEqual(13, len(lines))

    def test_json_array_becomes_json_lines(self):
        records = [{"employee_id": f"E{i:06d}"} for i in range(5)]
        args = self.args(write(self.folder, "in.json", json.dumps(records)), "JSON", 10)

        split_file.split(self.spark, args)

        lines = read_lines(os.path.join(args.output_dir, "*.json"))
        self.assertEqual(5, len(lines))
        self.assertEqual({"employee_id": "E000000"}, json.loads(sorted(lines)[0]))

    def test_empty_file_is_rejected(self):
        args = self.args(write(self.folder, "empty.csv", HEADER), "CSV", 10)

        with self.assertRaisesRegex(ValueError, "no records"):
            split_file.split(self.spark, args)


if __name__ == "__main__":
    unittest.main()
