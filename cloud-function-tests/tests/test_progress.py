import unittest
import requests
import os

BASE_URL = os.getenv("BASE_URL", "https://project-bdcc.ew.r.appspot.com")

class TestProgressEndpoint(unittest.TestCase):
    def test_filter_by_progress_id(self):
        """Test filtering by PROGRESS_ID."""
        response = requests.get(f"{BASE_URL}/rest/progress?PROGRESS_ID=1")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        for progress in data:
            self.assertEqual(progress["PROGRESS_ID"], 1)

    def test_filter_by_hadm_id(self):
        """Test filtering by HADM_ID."""
        response = requests.get(f"{BASE_URL}/rest/progress?HADM_ID=123456")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        for progress in data:
            self.assertEqual(progress["HADM_ID"], 123456)

    def test_filter_by_subject_id(self):
        """Test filtering by SUBJECT_ID."""
        response = requests.get(f"{BASE_URL}/rest/progress?SUBJECT_ID=10006")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        for progress in data:
            self.assertEqual(progress["SUBJECT_ID"], 10006)

    def test_filter_by_event_type(self):
        """Test filtering by EVENT_TYPE."""
        response = requests.get(f"{BASE_URL}/rest/progress?EVENT_TYPE=MEDICATION")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        for progress in data:
            self.assertEqual(progress["EVENT_TYPE"], "MEDICATION")

    def test_filter_by_event_datetime(self):
        """Test filtering by EVENT_DATETIME."""
        response = requests.get(f"{BASE_URL}/rest/progress?EVENT_DATETIME=2100-01-01")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        for progress in data:
            self.assertEqual(progress["EVENT_DATETIME"], "2100-01-01")

    def test_filter_by_status(self):
        """Test filtering by STATUS."""
        response = requests.get(f"{BASE_URL}/rest/progress?STATUS=COMPLETED")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        for progress in data:
            self.assertEqual(progress["STATUS"], "COMPLETED")

    def test_filter_by_multiple_columns(self):
        """Test filtering by multiple columns."""
        response = requests.get(f"{BASE_URL}/rest/progress?SUBJECT_ID=10006&STATUS=COMPLETED")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        for progress in data:
            self.assertEqual(progress["SUBJECT_ID"], 10006)
            self.assertEqual(progress["STATUS"], "COMPLETED")