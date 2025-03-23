import unittest
import requests
import os

BASE_URL = os.getenv("BASE_URL", "https://project-bdcc.ew.r.appspot.com")

class TestMedia(unittest.TestCase):
    def test(self):
        return True
       
    

if __name__ == "__main__":
    unittest.main()