#!/usr/bin/env python3
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from validate_etf_csv import validate_one


class ValidateEtfCsvTests(unittest.TestCase):
    def write_file(self, content: str) -> Path:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        path = Path(temp_dir.name) / "sample.csv"
        path.write_text(content, encoding="utf-8")
        return path

    def test_valid_file_passes(self):
        path = self.write_file(
            "date,open,high,low,close\n"
            "2024-01-02,1.0,1.1,0.9,1.05\n"
            "2024-01-03,1.05,1.2,1.0,1.1\n"
        )

        ok, errors, warnings = validate_one(path)

        self.assertTrue(ok)
        self.assertEqual(errors, [])
        self.assertEqual(warnings, [])

    def test_invalid_high_value_fails(self):
        path = self.write_file(
            "date,open,high,low,close\n"
            "2024-01-02,1.0,abc,0.9,1.05\n"
        )

        ok, errors, warnings = validate_one(path)

        self.assertFalse(ok)
        self.assertIn("字段 high 存在非数字值", errors)
        self.assertEqual(warnings, [])

    def test_unsorted_dates_warn(self):
        path = self.write_file(
            "date,open,high,low,close\n"
            "2024-01-03,1.0,1.1,0.9,1.05\n"
            "2024-01-02,1.05,1.2,1.0,1.1\n"
        )

        ok, errors, warnings = validate_one(path)

        self.assertTrue(ok)
        self.assertEqual(errors, [])
        self.assertIn("日期未按升序排列", warnings)


if __name__ == "__main__":
    unittest.main()
