import concurrent.futures

from flask import Flask, jsonify, request, redirect
from google.cloud import bigquery
from google.appengine.ext import blobstore
from google.appengine.ext import ndb
from google.appengine.api import wrap_wsgi_app

app = Flask(__name__)
bigquery_client = bigquery.Client()
app.wsgi_app = wrap_wsgi_app(app.wsgi_app, use_deferred=True)



@app.route("/")
def querylimits():
    query_job = bigquery_client.query(
        """
        SELECT
            *
        FROM `project-bdcc.MIMIC.ADMISSIONS`
        LIMIT 10
        """
    )

    rows = query_job.result()

    data = [ {"subject_id": row["SUBJECT_ID"] } for row in rows ]

    return jsonify(data)

@app.route("/rest/user", methods=["GET","POST","DELETE"])
def user():
    if request.method == "GET":
        patient_id = request.args.get('patient_id', type=int)

        query_job = bigquery_client.query(
            """
            SELECT *
            FROM `project-bdcc.MIMIC.PATIENTS`
            WHERE SUBJECT_ID=%d
            """ % patient_id
        )
        rows = query_job.result()
        if (rows.total_rows != 1):
            return "Patient doesnt exist"

        data = {}
        # Find a way to fetch only the row instead of iterating over it
        for row in rows:
            data = {"subject_id":row["SUBJECT_ID"], "gender":row["GENDER"],"dob":row["DOB"],"dod":row["DOD"]}

        return jsonify(data)

    elif request.method== "POST":
        patient_id = request.args.get('patient_id',type=int)
        query_job = bigquery_client.query(
            """
            SELECT *
            FROM `project-bdcc.MIMIC.PATIENTS`
            WHERE SUBJECT_ID=%d
            """ % patient_id
        )
        rows = query_job.result()
        if (rows.total_rows != 1):
            return "Patient doesnt exist"

        update_data = request.form
        update_values = ""
        for camp in update_data.keys():
            update_values += camp + "=" + update_data[camp] + ","
        update_values = update_values[:-1] # Remove the last comma

        query_job = bigquery_client.query(
            """
            UPDATE `project-bdcc.MIMIC.PATIENTS`
            SET %s
            WHERE SUBJECT_ID=%d
            """ % (update_values,patient_id)
        )
        rows = query_job.result()
        return redirect("/rest/user?patient_id=%d" % patient_id)

    elif request.method == "DELETE":
        patient_id = request.args.get('patient_id', type=int)
        query_job = bigquery_client.query(
            """
            SELECT *
            FROM `project-bdcc.MIMIC.PATIENTS`
            WHERE SUBJECT_ID=%d
            """ % patient_id
        )
        rows = query_job.result()
        if (rows.total_rows != 1):
            return "Patient doesnt exist"

        query_job = bigquery_client.query(
            """
            BEGIN TRANSACTION;
            DELETE FROM `project-bdcc.MIMIC.PATIENTS` WHERE SUBJECT_ID=%d;
            UPDATE `project-bdcc.MIMIC.ADMISSIONS` SET SUBJECT_ID=-1 WHERE SUBJECT_ID=%d;
            UPDATE `project-bdcc.MIMIC.LABEVENTS` SET SUBJECT_ID=-1 WHERE SUBJECT_ID=%d;
            UPDATE `project-bdcc.MIMIC.INPUTEVENTS` SET SUBJECT_ID=-1 WHERE SUBJECT_ID=%d;
            COMMIT TRANSACTION;
            """ % (patient_id,patient_id,patient_id,patient_id)
        )

        query_job.result()
        return "Patiend %d deleted" % patient_id


@app.route("/listprogress/<patient_id>")
def get_progress(patient_id):
    query_job = bigquery_client.query(
        """
        SELECT *
        FROM `project-bdcc.MIMIC.PATIENTS`
        WHERE SUBJECT_ID=%s
        """ % patient_id
    )

    rows = query_job.result()
    if (rows.total_rows != 1):
        return "Patient id doesn't exist"

    query_jobLABEVENTS = bigquery_client.query(
        """
        SELECT *
        FROM `project-bdcc.MIMIC.LABEVENTS`
        WHERE SUBJECT_ID=%s
        """ % patient_id
    )

    query_jobINPUTEVENTS = bigquery_client.query(
        """
        SELECT *
        FROM `project-bdcc.MIMIC.INPUTEVENTS`
        WHERE SUBJECT_ID=%s
        """ % patient_id
    )

    data = {"labevents" : [] , "inputevents": []}

    rows = query_jobLABEVENTS.result()
    for row in rows:
        r = {"itemid":row["ITEMID"], "value":row["VALUE"], "valueuom":row["VALUEUOM"], "flag":row["FLAG"]}
        data["labevents"].append(r)

    rows = query_jobINPUTEVENTS.result()
    for row in rows:
        r = {"itemid":row["ITEMID"],"starttime":row["STARTTIME"], "endtime":row["ENDTIME"], "amount":row["AMOUNT"], "amountuom":row["AMOUNTUOM"], "cgid":row["CGID"]}
        data["inputevents"].append(r)

    return jsonify(data)

@app.route("/longestwaitingtimes")
def get_longestwaiting():
    query_job = bigquery_client.query(
        """
        SELECT SUBJECT_ID,DATE_DIFF(DISCHTIME, ADMITTIME, day) as TIMEPASSED
        FROM `project-bdcc.MIMIC.ADMISSIONS`
        ORDER BY 2 DESC
        LIMIT 10
        """
    )

    rows = query_job.result()
    data = [ (row["SUBJECT_ID"],row["TIMEPASSED"]) for row in rows]

    return jsonify(data)

class UserMedia(ndb.Model):
    iduser = ndb.StringProperty()
    blob_key = ndb.BlobKeyProperty()
    upload_time = ndb.DateTimeProperty(auto_now_add=True)

@app.route("/mediauploadform")
def upload_media_form():
    """Formulário para upload de ficheiros."""
    upload_url = blobstore.create_upload_url("/mediauploaded_treatment")

    response = """
  <html><body>
  <form action="{}" method="POST" enctype="multipart/form-data">
    Upload File: <input type="file" name="file"><br>
    <input type="submit" name="submit" value="Submit Now">
  </form>
  </body></html>""".format(upload_url)

    return response

@app.route("/mediauploaded_treatment", methods=["POST"])
def upload_media_treatment():
    """Endpoint que recebe o upload do ficheiro"""
    return MediaUploadHandler().post()

class MediaUploadHandler(blobstore.BlobstoreUploadHandler):
    def post(self):
        """Handles file upload and stores blob key in BigQuery."""
        upload = self.get_uploads(request.environ)[0]  
        media = UserMedia(blob_key=upload.key(), iduser="1")
        media.put()

        return redirect("/userfiles/1/%s" % upload.key())

@app.route("/userfiles/1/<blob_key>")
def view_user_files(blob_key):
    return MediaDownloadHandler().get(blob_key)

class MediaDownloadHandler(blobstore.BlobstoreDownloadHandler):
    def get(self, media_key):
        if not blobstore.get(media_key):
            return "Photo key not found", 404
        else:
            headers = self.send_blob(request.environ, media_key)

            # Prevent Flask from setting a default content-type.
            # GAE sets it to a guessed type if the header is not set.
            headers["Content-Type"] = None
            return "", headers 

@app.route("/list_media")
def list_media():
    """Verifica os dados guardados no Datastore."""
    media = UserMedia.query().fetch()
    
    if not media:
        return "Nenhuma media encontrada no Datastore."

    return "<br>".join(
        [f"iduser: {media.iduser},\n Blob Key: {media.blob_key},\n Upload Time: {media.upload_time}" for media in media]
    )

if __name__ == "__main__":
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    app.run(host="127.0.0.1", port=8080, debug=True)
