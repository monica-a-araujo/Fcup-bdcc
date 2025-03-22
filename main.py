import concurrent.futures

from flask import Flask, jsonify, request, redirect
from google.cloud import bigquery
from google.appengine.ext import blobstore, ndb
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

# Endpoints that deals with CRUD operations on Users
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
            return "Patient doesn't exist"

        row = next(rows.__iter__())
        data = {"subject_id":row["SUBJECT_ID"], "gender":row["GENDER"],"dob":row["DOB"],"dod":row["DOD"]}

        return jsonify(data)

    # Insert a new patient into system. Can add the columns that want to insert like GENDER or DOB in multipart form.
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

        row = next(rows.__iter__(),None)
        row_id = row["R"] + 1 if row is not None else 0
        subject_id = row["S"] + 1 if row is not None else 0

        query_job = bigquery_client.query(
            """
            INSERT INTO `project-bdcc.MIMIC.PATIENTS` (%s,%s,%s)
            VALUES (%d,%d,%s)
            """ % ("ROW_ID","SUBJECT_ID",col_names,row_id,subject_id,col_values)
        )
        rows = query_job.result()

        return "Patient added with subject_id:%d" % subject_id

    # Update current patient's information. Columns and the respective values to update given in multipart form.
    elif request.method== "PUT":
        patient_id = request.args.get('patient_id',type=int)

        if (not user_exists(patient_id)):
            return "Patient %s doesn't exist" % patient_id

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

    # Delete patient. Update the admissions, labevents and inputevents to Default user which is -1.
    elif request.method == "DELETE":
        patient_id = request.args.get('patient_id', type=int)

        if (not user_exists(patient_id)):
            return "Patient %s doesn't exist" % patient_id

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

# Get the caregivers that treated the patient and can answer their questions
def get_possible_cgids(patient_id):
    query_job = bigquery_client.query(
        """
        SELECT DISTINCT(CGID)
        FROM `project-bdcc.MIMIC.INPUTEVENTS`
        WHERE SUBJECT_ID=%s
        """ % patient_id
    )

    rows = query_job.result()
    cgids = sorted([row["CGID"] for row in rows])

    return cgids

# This endpoint allows patients to see their questions and create new ones
@app.route("/rest/patients/<patient_id>/question",methods=["GET","POST"])
def handle_questions(patient_id):
    # Get list of questions ordered by their creation time
    if request.method == "GET":
        if (not user_exists(patient_id)):
            return "Patient %s doesn't exist" % patient_id

        query_job = bigquery_client.query(
            """
            SELECT *
            FROM `project-bdcc.MIMIC.Questions`
            WHERE PATIENT_ID=%s
            ORDER BY TIME_QUESTION ASC
            """ % patient_id
        )
        rows = query_job.result()

        questions = []
        for row in rows:
            q = {"question_id":row["QUESTION_ID"],"question":row["QUESTION"],"cgid":row["CGID"],"answer":row["ANSWER"],"time_question":row["TIME_QUESTION"],"time_answer":row["TIME_ANSWER"],"done":row["DONE"]}
            questions.append(q)

        return jsonify(questions)

    # Place a new question. This question can only be answered by a caregiver that treated this patient.
    # Only requires param QUESTION:String and CGID:String in multipart form
    elif request.method == "POST":
        question_data = request.form

        if (not user_exists(patient_id)):
            return "Patient %s doesn't exist" % patient_id

        # Test if cgid is valid, if not return the possible ones to use
        possible_cgids = get_possible_cgids(patient_id)
        if int(question_data["CGID"]) not in possible_cgids:
            return "Caregiver id:%s not valid. Please use one of the following: %s" % (question_data["CGID"],str(possible_cgids))

        # Get question_id to use for next question since big_query doesn't support auto increment
        query_job = bigquery_client.query(
            """
            SELECT MAX(QUESTION_ID) as MAX
            FROM `project-bdcc.MIMIC.Questions`
            """
        )
        rows = query_job.result()
        row = next(rows.__iter__())
        question_id = row["MAX"] + 1 if row is not None else 0

        query_job = bigquery_client.query(
            """
            INSERT INTO `project-bdcc.MIMIC.Questions` (QUESTION_ID,PATIENT_ID,QUESTION,CGID,DONE,TIME_QUESTION)
            VALUES (%d,%s,"%s",%s,FALSE,CURRENT_TIMESTAMP)
            """ % (question_id,patient_id,question_data["QUESTION"],question_data["CGID"])
        )
        query_job.result()
        return "Question added for patient %s" % patient_id

#Endpoint that allows caregivers to see the patients questions and answer to them
@app.route("/rest/caregivers/<cgid>/question",methods=["GET","PUT"])
def handle_caregivers(cgid):
    if request.method == "GET":
        query_job = bigquery_client.query(
            """
            SELECT *
            FROM `project-bdcc.MIMIC.Questions`
            WHERE CGID=%s
            ORDER BY QUESTION_ID
            """ % cgid
        )
        rows = query_job.result()

        questions = []
        for row in rows:
            q = {"question_id":row["QUESTION_ID"],"patient_id":row["PATIENT_ID"],"question":row["QUESTION"],"answer":row["ANSWER"],"time_question":row["TIME_QUESTION"],"time_answer":row["TIME_ANSWER"],"done":row["DONE"]}
            questions.append(q)

        return jsonify(questions)

    # Answer to a question. This question can only be answered by a caregiver that treated this patient.
    # Only requires param ANSWER:String and QUESTION_ID:String in multipart form
    elif request.method == "PUT":
        answer_data = request.form

        # Check if the cgid can answer the question
        query_job = bigquery_client.query(
            """
            SELECT *
            FROM `project-bdcc.MIMIC.Questions`
            WHERE QUESTION_ID=%s
            """ % answer_data["QUESTION_ID"]
        )
        rows = query_job.result()
        row = next(rows.__iter__(),None)
        if (row is None):
            return "Question %s doesn't exist" % answer_data["QUESTION_ID"]
        elif (row["DONE"] == True):
            return "Question already answered"
        elif (int(row["CGID"]) != int(cgid)):
            return "Invalid cgid. CGID:%s is answering but should be CGID:%s" % (cgid,row["CGID"])

        query_job = bigquery_client.query(
            """
            UPDATE `project-bdcc.MIMIC.Questions`
            SET ANSWER="%s", TIME_ANSWER=CURRENT_TIMESTAMP, DONE=TRUE
            WHERE QUESTION_ID=%s
            """ % (answer_data["ANSWER"],answer_data["QUESTION_ID"])
        )

        row = query_job.result()

        return "Question answered"

# Endpoints that gives the list of medical intervenction or tests
@app.route("/listprogress/<patient_id>")
def get_progress(patient_id):

    if (not user_exists(patient_id)):
        return "Patient %s doesn't exist" % patient_id

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

# Endpoint for google function to trigger a update to the longest waiting times
@app.route("/updatelongestwaitingtimes")
def update_longestwaiting():
    query_job = bigquery_client.query(
        """
        DELETE `project-bdcc.MIMIC.WaitingTimes` WHERE TRUE;
        INSERT INTO `project-bdcc.MIMIC.WaitingTimes` SELECT SUBJECT_ID,DATE_DIFF(DISCHTIME, ADMITTIME, day) as TIMEPASSED
        FROM `project-bdcc.MIMIC.ADMISSIONS`
        ORDER BY 2 DESC
        LIMIT 10
        """
    )
    rows = query_job.result()
    return "Updated Waiting times"

# Endpoint that gives the 10 patients that waited the most. This endpoint doesn't run a query every time is it called but relies
# on a information that is periodically updated by a google cloud function.
@app.route("/longestwaitingtimes")
def get_longestwaiting():
    query_job = bigquery_client.query(
        """
        SELECT * FROM `project-bdcc.MIMIC.WaitingTimes`
        ORDER BY TIMEPASSED DESC
        """
    )

    rows = query_job.result()
    data = [ (row["SUBJECT_ID"],row["TIMEPASSED"]) for row in rows]

    return jsonify(data)

#---

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

@app.route("/list_media/<iduser>")
def list_user_media(iduser):
    """Verifica os dados guardados no Datastore."""
    
    if not user_exists(iduser):
        return "User not found", 404

    media = UserMedia.query(UserMedia.iduser == iduser).fetch()

    if not media:
        notfound = "Nenhuma media encontrada para o user {}.".format(iduser)
        return notfound

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

# Helper function to convert BigQuery row to dictionary
def row_to_dict(row):
    return dict(row)

@app.route("/rest/admissions", methods=["GET", "POST", "PUT"])
def admissions():
    if request.method == "GET":
        try:
            query = """
            SELECT ROW_ID, HADM_ID, SUBJECT_ID, ADMITTIME, DISCHTIME, STATUS
            FROM `project-bdcc.MIMIC.ADMISSIONS`
            """
            conditions = []
            query_params = []

            # Add filters based on query parameters
            for key, value in request.args.items():
                if key == "SUBJECT_ID":
                    conditions.append("SUBJECT_ID = @subject_id")
                    query_params.append(bigquery.ScalarQueryParameter("subject_id", "INT64", int(value)))
                elif key == "STATUS":
                    conditions.append("STATUS = @status")
                    query_params.append(bigquery.ScalarQueryParameter("status", "STRING", value))
                elif key == "HADM_ID":
                    conditions.append("HADM_ID = @hadm_id")
                    query_params.append(bigquery.ScalarQueryParameter("hadm_id", "INT64", value))
                elif key == "ADMITTIME":
                    conditions.append("ADMITTIME = @admittime")
                    query_params.append(bigquery.ScalarQueryParameter("admittime", "TIMESTAMP", value))
                elif key == "DISCHTIME":
                    conditions.append("DISCHTIME = @dischtime")
                    query_params.append(bigquery.ScalarQueryParameter("dischtime", "TIMESTAMP", value))
                elif key == "all":
                    continue  # Ignore "all" parameter
                else:
                    return jsonify({"error": f"Unsupported filter: {key}"}), 400

            if conditions:
                query += " WHERE " + " AND ".join(conditions)

            # Execute the query
            job_config = bigquery.QueryJobConfig()
            if query_params:
                job_config.query_parameters = query_params
            query_job = bigquery_client.query(query, job_config=job_config)
            rows = query_job.result()

            # Convert rows to a list of dictionaries
            admissions = [row_to_dict(row) for row in rows]
            return jsonify(admissions), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    elif request.method == "POST":
        # Handle POST request to create a new admission
        try:
            data = request.get_json()
            if not data:
                return jsonify({"error": "Request body is empty"}), 400

            # Build the INSERT query dynamically
            columns = []
            values = []
            query_params = []
            for key, value in data.items():
                columns.append(key)
                values.append(f"@{key}")
                if isinstance(value, int):
                    query_params.append(bigquery.ScalarQueryParameter(key, "INT64", value))
                elif isinstance(value, str):
                    query_params.append(bigquery.ScalarQueryParameter(key, "STRING", value))
                else:
                    return jsonify({"error": f"Unsupported data type for column: {key}"}), 400

            query = f"""
            INSERT INTO `project-bdcc.MIMIC.ADMISSIONS` ({', '.join(columns)})
            VALUES ({', '.join(values)})
            """

            # Execute the query
            job_config = bigquery.QueryJobConfig()
            job_config.query_parameters = query_params
            query_job = bigquery_client.query(query, job_config=job_config)
            query_job.result()  # Wait for the query to complete

            return jsonify({"message": "Admission created successfully"}), 201
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    elif request.method == "PUT":
        # Handle PUT request to update an admission
        try:
            hadm_id = request.args.get('HADM_ID', type=int)
            if not hadm_id:
                return jsonify({"error": "HADM_ID is required"}), 400

            data = request.get_json()
            if not data:
                return jsonify({"error": "Request body is empty"}), 400

            # Build the UPDATE query dynamically
            update_fields = []
            query_params = []
            for key, value in data.items():
                update_fields.append(f"{key} = @{key}")
                if isinstance(value, int):
                    query_params.append(bigquery.ScalarQueryParameter(key, "INT64", value))
                elif isinstance(value, str):
                    query_params.append(bigquery.ScalarQueryParameter(key, "STRING", value))
                else:
                    return jsonify({"error": f"Unsupported data type for column: {key}"}), 400

            query = f"""
            UPDATE `project-bdcc.MIMIC.ADMISSIONS`
            SET {', '.join(update_fields)}
            WHERE HADM_ID = @hadm_id
            """
            query_params.append(bigquery.ScalarQueryParameter("hadm_id", "INT64", hadm_id))

            # Execute the query
            job_config = bigquery.QueryJobConfig()
            job_config.query_parameters = query_params
            query_job = bigquery_client.query(query, job_config=job_config)
            query_job.result()  # Wait for the query to complete

            return jsonify({"message": "Admission updated successfully"}), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 500

@app.route("/rest/progress", methods=["GET", "POST", "PUT"])
def progress():
    if request.method == "GET":
        # Handle GET request to retrieve progress entries
        try:
            query = """
            SELECT PROGRESS_ID, HADM_ID, SUBJECT_ID, EVENT_TYPE, EVENT_DATETIME, DESCRIPTION, VALUE, VALUE_NUM, VALUE_UOM, STATUS, CREATED_AT
            FROM `project-bdcc.MIMIC.Progress`
            """
            conditions = []
            query_params = []

            # Add filters based on query parameters
            for key, value in request.args.items():
                if key == "SUBJECT_ID":
                    conditions.append("SUBJECT_ID = @subject_id")
                    query_params.append(bigquery.ScalarQueryParameter("subject_id", "INT64", int(value)))
                elif key == "STATUS":
                    conditions.append("STATUS = @status")
                    query_params.append(bigquery.ScalarQueryParameter("status", "STRING", value))
                elif key == "HADM_ID":
                    conditions.append("HADM_ID = @hadm_id")
                    query_params.append(bigquery.ScalarQueryParameter("hadm_id", "INT64", value))
                elif key == "PROGRESS_ID":
                    conditions.append("PROGRESS_ID = @progress_id")
                    query_params.append(bigquery.ScalarQueryParameter("progress_id", "INT64", value))
                elif key == "EVENT_TYPE":
                    conditions.append("EVENT_TYPE = @event_type")
                    query_params.append(bigquery.ScalarQueryParameter("event_type", "STRING", value))
                elif key == "EVENT_DATETIME":
                    conditions.append("EVENT_DATETIME = @event_datetime")
                    query_params.append(bigquery.ScalarQueryParameter("event_datetime", "TIMESTAMP", value))
                elif key == "DESCRIPTION":
                    conditions.append("DESCRIPTION = @description")
                    query_params.append(bigquery.ScalarQueryParameter("description", "STRING", value))
                elif key == "VALUE":
                    conditions.append("VALUE = @value")
                    query_params.append(bigquery.ScalarQueryParameter("value", "STRING", value))
                elif key == "CREATED_AT":
                    conditions.append("CREATED_AT = @created_at")
                    query_params.append(bigquery.ScalarQueryParameter("created_at", "TIMESTAMP", value))
                elif key == "all":
                    continue  # Ignore "all" parameter
                else:
                    return jsonify({"error": f"Unsupported filter: {key}"}), 400

            if conditions:
                query += " WHERE " + " AND ".join(conditions)

            # Execute the query
            job_config = bigquery.QueryJobConfig()
            if query_params:
                job_config.query_parameters = query_params
            query_job = bigquery_client.query(query, job_config=job_config)
            rows = query_job.result()

            # Convert rows to a list of dictionaries
            progress = [row_to_dict(row) for row in rows]
            return jsonify(progress), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    elif request.method == "POST":
        # Handle POST request to create a new progress entry
        try:
            data = request.get_json()
            if not data:
                return jsonify({"error": "Request body is empty"}), 400

            # Query the maximum PROGRESS_ID and increment it by 1
            query_max_id = """
            SELECT MAX(PROGRESS_ID) as max_id
            FROM `project-bdcc.MIMIC.Progress`
            """
            query_job = bigquery_client.query(query_max_id)
            result = query_job.result()
            row = next(result, None)  # Get the first row or None if no rows exist
            max_id = row["max_id"] if row and row["max_id"] is not None else 0  # Default to 0 if no rows exist
            progress_id = max_id + 1

            # Build the INSERT query dynamically
            columns = ["PROGRESS_ID"]  # Add PROGRESS_ID as the first column
            values = [f"@progress_id"]  # Add PROGRESS_ID as the first value
            query_params = [bigquery.ScalarQueryParameter("progress_id", "INT64", progress_id)]

            for key, value in data.items():
                columns.append(key)
                values.append(f"@{key}")
                if isinstance(value, int):
                    query_params.append(bigquery.ScalarQueryParameter(key, "INT64", value))
                elif isinstance(value, str):
                    query_params.append(bigquery.ScalarQueryParameter(key, "STRING", value))
                elif isinstance(value, float):
                    query_params.append(bigquery.ScalarQueryParameter(key, "FLOAT64", value))
                else:
                    return jsonify({"error": f"Unsupported data type for column: {key}"}), 400

            query = f"""
            INSERT INTO `project-bdcc.MIMIC.Progress` ({', '.join(columns)})
            VALUES ({', '.join(values)})
            """

            # Execute the query
            job_config = bigquery.QueryJobConfig()
            job_config.query_parameters = query_params
            query_job = bigquery_client.query(query, job_config=job_config)
            query_job.result()  # Wait for the query to complete

            return jsonify({"message": "Progress entry created successfully", "PROGRESS_ID": progress_id}), 201
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    elif request.method == "PUT":
        # Handle PUT request to update a progress entry
        try:
            progress_id = request.args.get('PROGRESS_ID', type=int)
            if not progress_id:
                return jsonify({"error": "PROGRESS_ID is required"}), 400

            data = request.get_json()
            if not data:
                return jsonify({"error": "Request body is empty"}), 400

            # Build the UPDATE query dynamically
            update_fields = []
            query_params = []
            for key, value in data.items():
                update_fields.append(f"{key} = @{key}")
                if isinstance(value, int):
                    query_params.append(bigquery.ScalarQueryParameter(key, "INT64", value))
                elif isinstance(value, str):
                    query_params.append(bigquery.ScalarQueryParameter(key, "STRING", value))
                elif isinstance(value, float):
                    query_params.append(bigquery.ScalarQueryParameter(key, "FLOAT64", value))
                else:
                    return jsonify({"error": f"Unsupported data type for column: {key}"}), 400

            query = f"""
            UPDATE `project-bdcc.MIMIC.Progress`
            SET {', '.join(update_fields)}
            WHERE PROGRESS_ID = @progress_id
            """
            query_params.append(bigquery.ScalarQueryParameter("progress_id", "INT64", progress_id))

            # Execute the query
            job_config = bigquery.QueryJobConfig()
            job_config.query_parameters = query_params
            query_job = bigquery_client.query(query, job_config=job_config)
            query_job.result()  # Wait for the query to complete

            return jsonify({"message": "Progress entry updated successfully"}), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 500

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
