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
        response = requests.get(f"{BASE_URL}/rest/progress?EVENT_DATETIME=2023-10-01%2012:00:00%20UTC")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        for progress in data:
            self.assertEqual(progress["EVENT_DATETIME"], "2023-10-01%2012:00:00%20UTC")

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

    def test_create_progress(self):
        """Test creating a new progress entry via POST."""
        new_progress = {
            "HADM_ID": 123456,
            "SUBJECT_ID": 10007,
            "EVENT_TYPE": "MEDICATION",
            "EVENT_DATETIME": "2023-10-02 12:00:00 UTC",
            "STATUS": "PENDING"
        }

        # Send the POST request to create a new progress entry
        response = requests.post(f"{BASE_URL}/rest/progress", json=new_progress)
        self.assertEqual(response.status_code, 201)  # 201 Created

        # Parse the response data
        data = response.json()

        # Verify the response contains a success message
        self.assertIn("message", data)
        self.assertEqual(data["message"], "Progress entry created successfully")

    def test_update_progress(self):
        """Test updating an existing progress entry via PUT."""
        # First, create a new progress entry to update
        new_progress = {
            "HADM_ID": 123457,
            "SUBJECT_ID": 10008,
            "EVENT_TYPE": "LAB_TEST",
            "EVENT_DATETIME": "2023-10-03 12:00:00 UTC",
            "STATUS": "PENDING"
        }
        create_response = requests.post(f"{BASE_URL}/rest/progress", json=new_progress)
        self.assertEqual(create_response.status_code, 201)
        created_progress = create_response.json()

        # Verify the response contains a success message
        self.assertIn("message", created_progress)
        self.assertEqual(created_progress["message"], "Progress entry created successfully")

        # Update the progress entry using PROGRESS_ID as a query parameter
        updated_progress = {
            "HADM_ID": 123457,
            "SUBJECT_ID": 10008,
            "EVENT_TYPE": "LAB_TEST",
            "EVENT_DATETIME": "2023-10-03 12:00:00 UTC",
            "STATUS": "COMPLETED"  # Updated STATUS
        }
        update_response = requests.put(
            f"{BASE_URL}/rest/progress?PROGRESS_ID={created_progress.get('PROGRESS_ID', 'unknown')}",
            json=updated_progress
        )
        self.assertEqual(update_response.status_code, 200)
        updated_data = update_response.json()

        # Verify the response contains a success message
        self.assertIn("message", updated_data)
        self.assertEqual(updated_data["message"], "Progress entry updated successfully")
if __name__ == "__main__":
    unittest.main()