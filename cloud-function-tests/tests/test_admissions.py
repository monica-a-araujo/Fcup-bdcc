import unittest
import requests
import os

BASE_URL = os.getenv("BASE_URL", "https://project-bdcc.ew.r.appspot.com")

class TestAdmissionsEndpoint(unittest.TestCase):
    def test_filter_by_admittime(self):
        """Test filtering by ADMITTIME."""
        response = requests.get(f"{BASE_URL}/rest/admissions?ADMITTIME=2023-10-01%2012:00:00%20UTC")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        for admission in data:
            self.assertEqual(admission["ADMITTIME"], "Sun, 01 Oct 2023 12:00:00 GMT")  # Updated format

    def test_filter_by_dischtime(self):
        """Test filtering by DISCHTIME."""
        response = requests.get(f"{BASE_URL}/rest/admissions?DISCHTIME=2023-10-01%2012:00:00%20UTC")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        for admission in data:
            self.assertEqual(admission["DISCHTIME"], "Sun, 01 Oct 2023 12:00:00 GMT")  # Updated format

    def test_filter_by_subject_id(self):
        """Test filtering by SUBJECT_ID."""
        response = requests.get(f"{BASE_URL}/rest/admissions?SUBJECT_ID=10006")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        for admission in data:
            self.assertEqual(admission["SUBJECT_ID"], 10006)

    def test_filter_by_status(self):
        """Test filtering by STATUS."""
        response = requests.get(f"{BASE_URL}/rest/admissions?STATUS=active")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        for admission in data:
            self.assertEqual(admission["STATUS"], "active")

    def test_filter_by_multiple_columns(self):
        """Test filtering by multiple columns."""
        response = requests.get(f"{BASE_URL}/rest/admissions?SUBJECT_ID=10006&STATUS=active")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        for admission in data:
            self.assertEqual(admission["SUBJECT_ID"], 10006)
            self.assertEqual(admission["STATUS"], "active")

    def test_create_admission(self):
        """Test creating a new admission via POST."""
        new_admission = {
            "ROW_ID": 123456,
            "SUBJECT_ID": 10007,
            "ADMITTIME": "2023-10-02 12:00:00 UTC",
            "DISCHTIME": "2023-10-03 12:00:00 UTC",
            "STATUS": "active"
        }

        # Send the POST request to create a new admission
        response = requests.post(f"{BASE_URL}/rest/admissions", json=new_admission)
        self.assertEqual(response.status_code, 201)  # 201 Created

        # Parse the response data
        data = response.json()

        # Verify the response contains a success message
        self.assertIn("message", data)
        self.assertEqual(data["message"], "Admission created successfully")

if __name__ == "__main__":
    unittest.main()