gcloud scheduler jobs create http update_waiting_times --schedule="* * * * *" --uri="https://project-bdcc.ew.r.appspot.com/updatelongestwaitingtimes" --http-method=GET
