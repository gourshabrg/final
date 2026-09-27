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


def read_contract():
    """contracts/lumi-contract.json: mounted in the container, or next to this repo on the host."""
    for path in ("/opt/lumi/contracts/lumi-contract.json",
                 os.path.join(os.path.dirname(__file__), "..", "..", "contracts", "lumi-contract.json")):
        if os.path.isfile(path):
            with open(path, encoding="utf-8") as file:
                return json.load(file)
    raise FileNotFoundError("lumi-contract.json not found")


class ContractTest(unittest.TestCase):
    """The split job's names must match what Beam reads (see contracts/lumi-contract.json)."""

    def test_file_types_and_split_columns_match_the_contract(self):
        contract = read_contract()
        self.assertEqual(tuple(contract["file_types"]), split_file.FILE_TYPES)
        self.assertEqual(contract["split_columns"]["source_record_number"], split_file.SOURCE_RECORD_NUMBER)
        self.assertEqual(contract["split_columns"]["corrupt_record"], split_file.CORRUPT_RECORD)


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
        self.assertEqual(3, lines.count("employee_id,first_name,_corrupt_record,source_record_number"))
        self.assertEqual(13, len(lines))

    def test_json_array_becomes_json_lines(self):
        records = [{"employee_id": f"E{i:06d}"} for i in range(5)]
        args = self.args(write(self.folder, "in.json", json.dumps(records)), "JSON", 10)

        split_file.split(self.spark, args)

        lines = read_lines(os.path.join(args.output_dir, "*.json"))
        self.assertEqual(5, len(lines))
        self.assertEqual({"employee_id": "E000000", "source_record_number": 1}, json.loads(sorted(lines)[0]))

    def test_every_record_keeps_its_position_in_the_original_file(self):
        rows = "".join(f"E{i:06d},Name{i}\n" for i in range(1, 121))
        args = self.args(write(self.folder, "in.csv", HEADER + rows), "CSV", 50)

        split_file.split(self.spark, args)

        # Row "E000087,Name87" was record 87 of the original file, whichever part file it landed in.
        records = [line.split(",") for line in read_lines(os.path.join(args.output_dir, "*.csv"))
                   if not line.startswith("employee_id")]
        self.assertEqual(120, len(records))
        for employee_id, _, _, number in records:
            self.assertEqual(int(employee_id[1:]), int(number))

    def test_json_records_keep_their_position(self):
        records = [{"employee_id": f"E{i:06d}"} for i in range(1, 8)]
        args = self.args(write(self.folder, "in.json", json.dumps(records)), "JSON", 3)

        split_file.split(self.spark, args)

        for line in read_lines(os.path.join(args.output_dir, "*.json")):
            record = json.loads(line)
            self.assertEqual(int(record["employee_id"][1:]), record["source_record_number"])

    def test_broken_json_line_is_marked_corrupt(self):
        text = '{"employee_id": "E1"}\n{"employee_id": broken\n{"employee_id": "E3"}\n'
        args = self.args(write(self.folder, "in.jsonl", text), "JSON", 10)

        split_file.split(self.spark, args)

        records = sorted((json.loads(line) for line in read_lines(os.path.join(args.output_dir, "*.json"))),
                         key=lambda record: record["source_record_number"])
        self.assertEqual({"_corrupt_record": '{"employee_id": broken', "source_record_number": 2}, records[1])
        self.assertEqual("E3", records[2]["employee_id"])

    def test_json_values_keep_their_original_types(self):
        # Spark's JSON reader would turn every salary into a decimal because of the one 1.5.
        records = [{"employee_id": "E1", "salary": 950000}, {"employee_id": "E2", "salary": 1.5}]
        args = self.args(write(self.folder, "in.json", json.dumps(records)), "JSON", 10)

        split_file.split(self.spark, args)

        salaries = {json.loads(line)["employee_id"]: json.loads(line)["salary"]
                    for line in read_lines(os.path.join(args.output_dir, "*.json"))}
        self.assertEqual({"E1": 950000, "E2": 1.5}, salaries)
        self.assertIsInstance(salaries["E1"], int)

    def test_invalid_json_array_fails_with_a_clear_message(self):
        args = self.args(write(self.folder, "in.json", '[{"employee_id": "E1"}, {oops'), "JSON", 10)

        with self.assertRaisesRegex(ValueError, "not valid JSON"):
            split_file.split(self.spark, args)

    def test_quotes_and_line_breaks_inside_csv_values_survive(self):
        text = (HEADER.replace("first_name", "job_title")
                + 'E000001,"Lead ""Data"" Eng"\n'
                + 'E000002,"Multi\nline"\n')
        args = self.args(write(self.folder, "in.csv", text), "CSV", 1)

        self.assertEqual(2, split_file.split(self.spark, args))

        content = "\n".join(read_lines(os.path.join(args.output_dir, "*.csv")))
        self.assertIn('E000001,"Lead ""Data"" Eng",,1', content)
        self.assertIn('E000002,"Multi\nline",,2', content)

    def test_row_with_wrong_column_count_is_marked_not_fixed(self):
        text = HEADER + "E000001,Ann\nE000002,Bob,EXTRA\nE000003\n"
        args = self.args(write(self.folder, "in.csv", text), "CSV", 10)

        split_file.split(self.spark, args)

        # Good row: empty _corrupt_record; broken rows keep their raw text.
        lines = read_lines(os.path.join(args.output_dir, "*.csv"))
        self.assertIn("E000001,Ann,,1", lines)
        self.assertIn("E000002,Bob,\"E000002,Bob,EXTRA\",2", lines)
        self.assertIn("E000003,,E000003,3", lines)

    def test_empty_file_is_rejected(self):
        args = self.args(write(self.folder, "empty.csv", HEADER), "CSV", 10)

        with self.assertRaisesRegex(ValueError, "no records"):
            split_file.split(self.spark, args)


if __name__ == "__main__":
    unittest.main()
