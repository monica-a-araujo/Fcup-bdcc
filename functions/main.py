# The code of a GCF triggered by a event
import requests

def handle_event(event,message):
    requests.get("https://project-bdcc.ew.r.appspot.com/updatelongestwaitingtimes")
    print("Updated")