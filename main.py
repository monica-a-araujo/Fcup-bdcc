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

@app.route("/rest/user", methods=["GET","POST", "PUT","DELETE"])
def user():
    # Get information of a patient
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

        row = next(rows.__iter__())
        data = {"subject_id":row["SUBJECT_ID"], "gender":row["GENDER"],"dob":row["DOB"],"dod":row["DOD"]}

        return jsonify(data)

    # Insert a new patient into system
    elif request.method == "POST":

        insert_data = request.form
        col_names = ",".join(insert_data.keys())
        col_values = ",".join(insert_data.values())

        query_job = bigquery_client.query(
            """
            SELECT MAX(ROW_ID) AS R, MAX(SUBJECT_ID) AS S
            FROM `project-bdcc.MIMIC.PATIENTS`
            """
        )

        rows = query_job.result()

        row = next(rows.__iter__())
        row_id = row["R"] + 1
        subject_id = row["S"] + 1

        query_job = bigquery_client.query(
            """
            INSERT INTO `project-bdcc.MIMIC.PATIENTS` (%s,%s,%s)
            VALUES (%d,%d,%s)
            """ % ("ROW_ID","SUBJECT_ID",col_names,row_id,subject_id,col_values)
        )
        rows = query_job.result()

        return "Patient added with subject_id:%d" % subject_id


    # Update current patient's information
    elif request.method== "PUT":
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
        return "Patient updated"

    # Delete patient
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

# ----

class UserMedia(ndb.Model):
    iduser = ndb.StringProperty()
    blob_key = ndb.BlobKeyProperty()
    upload_time = ndb.DateTimeProperty(auto_now_add=True)

@app.route("/mediauploadform/<iduser>")
def upload_media_form(iduser):
    """Formulário para upload de ficheiros."""

    if not user_exists(iduser):
        return "User not found", 404
    
    upload_url = blobstore.create_upload_url(f"/mediauploaded_treatment/{iduser}")

    response = """
  <html><body>
  <form action="{}" method="POST" enctype="multipart/form-data">
    Upload File: <input type="file" name="file"><br>
    <input type="submit" name="submit" value="Submit Now">
  </form>
  </body></html>""".format(upload_url)

    return response

def user_exists(iduser):
    query = f"""
        SELECT COUNT(*) as count
        FROM `project-bdcc.MIMIC.PATIENTS`
        WHERE SUBJECT_ID = {iduser}
    """
    query_job = bigquery_client.query(query)
    result = query_job.result()

    row = next(result, None)
    return row["count"] > 0 if row else False

@app.route("/mediauploaded_treatment/<iduser>", methods=["POST"])
def upload_media_treatment(iduser):
    """Endpoint que recebe o upload do ficheiro"""
    return MediaUploadHandler().post(iduser)
    
class MediaUploadHandler(blobstore.BlobstoreUploadHandler):
    def post(self, iduser):
        """Handles file upload and stores blob key in BigQuery."""
        upload = self.get_uploads(request.environ)[0]  
        media = UserMedia(blob_key=upload.key(), iduser=iduser)
        media.put()

        return redirect("/usermedia/{}/{}".format(iduser, upload.key()))


@app.route("/usermedia/<iduser>/<blob_key>")
def view_user_files(iduser, blob_key):
    return MediaDownloadHandler(iduser).get(blob_key)

class MediaDownloadHandler(blobstore.BlobstoreDownloadHandler):
    def __init__(self, iduser):
        self.iduser = iduser 

    def get(self, media_key): #todo
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
        return "Nenhuma media encontrada."
    
    media_urls = [f"/usermedia/{item.iduser}/{item.blob_key}" for item in media]
    media_count = len(media)

    response = """<html><body>
        <h1>Media</h1>
        <h2>{} media uploaded:</h2>
        <ul>
        """.format(media_count)

    for index, item in enumerate(media):
        response += """
            <li>
                <h3>File {}:</h3>
                <p>- blob key: {}</p>
                <p>- User id: {}</p>
                <p>- Upload time: {}</p>
                <a href="{}">Download</a>
            </li>
            <br>
        """.format(index + 1, item.blob_key, item.iduser, item.upload_time, media_urls[index])

    response += """
        </ul>
        <p><a href="/mediauploadform/12">Return to upload form</a></p> 
        </body></html>
    """#testar
    
    return response

@app.route("/help")
def help_page():
    """Página de ajuda com links rápidos e input para ID do utilizador."""
    response = """
    <html>
    <head>
        <title>Help Page</title>
    </head>
    <body>
        <h1>Ajuda e Navegação</h1>
        <p>Use os links abaixo para aceder rapidamente às funcionalidades do sistema:</p>
        <ul>
            <li><a href="/">Página Inicial</a></li>
            <li><a href="/list_media">Ver Ficheiros</a></li>
            <li><a href="/rest/user?patient_id=1">Dados do Utilizador</a></li>
        </ul>
        <input type="number" id="user_id" placeholder="Introduza o ID do paciente">
        <button onclick="updateLink()">Gerar Link</button>
        <p><a id="upload_link" href="#">(O link para o upload media do paciente aparecerá aqui)</a></p>
        <p><a id="user_link" href="#">(O link para a consulta aparecerá aqui)</a></p>

        <script>
            function updateLink() {
                var userId = document.getElementById("user_id").value;
                if (userId) {
                    document.getElementById("upload_link").href = "/mediauploadform/" + userId;
                    document.getElementById("upload_link").innerText = "Upload de Ficheiros (ID: " + userId + ")";

                    document.getElementById("user_link").href = "/rest/user?patient_id=" + userId;
                    document.getElementById("user_link").innerText = "Consultar Utilizador (ID: " + userId + ")";
                } else {
                    alert("Por favor, introduza um ID válido.");
                }
            }
        </script>
    </body>
    </html>
    """
    return response



if __name__ == "__main__":
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    app.run(host="127.0.0.1", port=8080, debug=True)
