import unittest
from google.appengine.ext import testbed
from main import app, UserMedia
import requests

class MediaServiceTests(unittest.TestCase):
    
    def setUp(self):
        self.app = app.test_client()
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_blobstore_stub()

    def tearDown(self):
        self.testbed.deactivate()

    #form for media upload shows
    def test_upload_media_form(self):
        response = requests.get(f"https://project-bdcc.ew.r.appspot.com/mediauploadform/12345")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b'Upload File', response.data)

    def test_user_not_found(self):
        response = requests.get(f"https://project-bdcc.ew.r.appspot.com/mediauploadform/00000")
        self.assertEqual(response.status_code, 404)
        self.assertIn(b'User not found', response.data)
    
    def test_upload_media_treatment(self):
        data = {
            'file': (bytes('test example 123', 'utf-8'), 'test_file.txt')
        }
        media = UserMedia.query(UserMedia.iduser == "123456").fetch()
        len_media_before = len(media)
      
        response = requests.post(f"https://project-bdcc.ew.r.appspot.com/mediauploaded_treatment/123456", data=data)
        self.assertEqual(response.status_code, 302)
      
        media = UserMedia.query(UserMedia.iduser == "123456").fetch()
        len_media_after = len(media)
        self.assertEqual(len_media_before+1, len_media_after)
       
        
    
    def test_list_media_empty(self):
        response = requests.get(f"https://project-bdcc.ew.r.appspot.com/list_media")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b'Nenhuma media encontrada', response.data)
    
    def test_list_user_media_not_found(self):
        response = requests.get(f"https://project-bdcc.ew.r.appspot.com/list_media/00000")
        self.assertEqual(response.status_code, 404)
        self.assertIn(b'User not found', response.data)
    
    def test_list_user_media_success(self):
        media = UserMedia(iduser='22231', blob_key='fbejlfberj')
        media.put()

        response = requests.get(f"https://project-bdcc.ew.r.appspot.com/list_media/22231")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b'fbejlfberj', response.data)

if __name__ == '__main__':
    unittest.main()
