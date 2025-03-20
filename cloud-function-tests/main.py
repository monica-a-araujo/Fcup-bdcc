import unittest
from tests.test_admissions import TestAdmissionsEndpoint
from tests.test_progress import TestProgressEndpoint

def run_tests(request):
    """Cloud Function entry point."""
    try:
        # Create a test suite and add the test cases
        suite = unittest.TestSuite()
        suite.addTest(unittest.makeSuite(TestAdmissionsEndpoint))
        suite.addTest(unittest.makeSuite(TestProgressEndpoint))

        # Run the tests
        runner = unittest.TextTestRunner()
        result = runner.run(suite)

        # Check if all tests passed
        if result.wasSuccessful():
            return "All tests passed!", 200
        else:
            return "Some tests failed!", 500
    except Exception as e:
        return f"An error occurred: {str(e)}", 500