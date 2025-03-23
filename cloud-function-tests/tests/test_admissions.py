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
            self.assertEqual(admission["ADMITTIME"], "2023-10-01%2012:00:00%20UTC")

    def test_filter_by_dischtime(self):
        """Test filtering by DISCHTIME."""
        response = requests.get(f"{BASE_URL}/rest/admissions?DISCHTIME=2023-10-01%2012:00:00%20UTC")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        for admission in data:
            self.assertEqual(admission["DISCHTIME"], "2023-10-01%2012:00:00%20UTC")

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
            "SUBJECT_ID": 10007,
            "ADMITTIME": "2023-10-02 12:00:00 UTC",
            "DISCHTIME": "2023-10-03 12:00:00 UTC",
            "STATUS": "active"
        }
        response = requests.post(f"{BASE_URL}/rest/admissions", json=new_admission)
        self.assertEqual(response.status_code, 201)  # 201 Created
        data = response.json()
        self.assertEqual(data["SUBJECT_ID"], new_admission["SUBJECT_ID"])
        self.assertEqual(data["ADMITTIME"], new_admission["ADMITTIME"])
        self.assertEqual(data["DISCHTIME"], new_admission["DISCHTIME"])
        self.assertEqual(data["STATUS"], new_admission["STATUS"])

    def test_update_admission(self):
        """Test updating an existing admission via PUT."""
        # First, create a new admission to update
        new_admission = {
            "SUBJECT_ID": 10008,
            "ADMITTIME": "2023-10-04 12:00:00 UTC",
            "DISCHTIME": "2023-10-05 12:00:00 UTC",
            "STATUS": "active"
        }
        create_response = requests.post(f"{BASE_URL}/rest/admissions", json=new_admission)
        self.assertEqual(create_response.status_code, 201)
        created_admission = create_response.json()

        # Update the admission using HADM_ID as a query parameter
        updated_admission = {
            "SUBJECT_ID": 10008,
            "ADMITTIME": "2023-10-04 12:00:00 UTC",
            "DISCHTIME": "2023-10-06 12:00:00 UTC",  # Updated DISCHTIME
            "STATUS": "inactive"  # Updated STATUS
        }
        update_response = requests.put(
            f"{BASE_URL}/rest/admissions?HADM_ID={created_admission['HADM_ID']}",
            json=updated_admission
        )
        self.assertEqual(update_response.status_code, 200)
        updated_data = update_response.json()
        self.assertEqual(updated_data["DISCHTIME"], updated_admission["DISCHTIME"])
        self.assertEqual(updated_data["STATUS"], updated_admission["STATUS"])

if __name__ == "__main__":
    unittest.main()