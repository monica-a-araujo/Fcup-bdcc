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

    def test_update_admission(self):
        """Test updating an existing admission via PUT."""
        # First, create a new admission to update
        new_admission = {
            "ROW_ID": 123456,  
            "SUBJECT_ID": 10008,
            "ADMITTIME": "2023-10-04 12:00:00 UTC",
            "DISCHTIME": "2023-10-05 12:00:00 UTC",
            "STATUS": "active"
        }
        create_response = requests.post(f"{BASE_URL}/rest/admissions", json=new_admission)
        
        # Assert that the admission creation was successful
        self.assertEqual(create_response.status_code, 201)
        created_admission = create_response.json()

        # Verify the response contains a success message
        self.assertIn("message", created_admission)
        self.assertEqual(created_admission["message"], "Admission created successfully")

        # Prepare the updated admission data
        updated_admission = {
            "DISCHTIME": "2023-10-06 12:00:00 UTC",  # Updated DISCHTIME
            "STATUS": "inactive"  # Updated STATUS
        }
        
        # Update the admission using ROW_ID as a query parameter
        update_response = requests.put(
            f"{BASE_URL}/rest/admissions?ROW_ID={created_admission.get('ROW_ID', 'unknown')}",
            json=updated_admission
        )

        # Print the response for error diagnostics
        print(f"Update response status code: {update_response.status_code}")
        print(f"Error message: {update_response.text}")

        # Verify the response contains a success message
        if update_response.status_code == 200:
            updated_data = update_response.json()
            self.assertIn("message", updated_data)
            self.assertEqual(updated_data["message"], "Admission updated successfully")
        else:
            self.fail(f"Admission update failed with status code: {update_response.status_code}")

if __name__ == "__main__":
    unittest.main()