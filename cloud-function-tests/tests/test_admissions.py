import unittest
import requests
import os

BASE_URL = os.getenv("BASE_URL", "https://project-bdcc.ew.r.appspot.com")

class TestAdmissionsEndpoint(unittest.TestCase):
    def test_filter_by_subject_id(self):
        """Test filtering by SUBJECT_ID."""
        response = requests.get(f"{BASE_URL}/rest/admissions?SUBJECT_ID=10006")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        for admission in data:
            self.assertEqual(admission["SUBJECT_ID"], 10006)

    def test_filter_by_status(self):
        """Test filtering by STATUS."""
        response = requests.get(f"{BASE_URL}/rest/admissions?STATUS=ALIVE")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        for admission in data:
            self.assertEqual(admission["STATUS"], "ALIVE")

    def test_filter_by_hadm_id(self):
        """Test filtering by HADM_ID."""
        response = requests.get(f"{BASE_URL}/rest/admissions?HADM_ID=123456")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        for admission in data:
            self.assertEqual(admission["HADM_ID"], 123456)

    def test_filter_by_admittime(self):
        """Test filtering by ADMITTIME."""
        response = requests.get(f"{BASE_URL}/rest/admissions?ADMITTIME=2100-01-01")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        for admission in data:
            self.assertEqual(admission["ADMITTIME"], "2100-01-01")

    def test_filter_by_dischtime(self):
        """Test filtering by DISCHTIME."""
        response = requests.get(f"{BASE_URL}/rest/admissions?DISCHTIME=2100-01-02")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        for admission in data:
            self.assertEqual(admission["DISCHTIME"], "2100-01-02")

    def test_filter_by_multiple_columns(self):
        """Test filtering by multiple columns."""
        response = requests.get(f"{BASE_URL}/rest/admissions?SUBJECT_ID=10006&STATUS=ALIVE")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        for admission in data:
            self.assertEqual(admission["SUBJECT_ID"], 10006)
            self.assertEqual(admission["STATUS"], "ALIVE")