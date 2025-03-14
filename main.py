import concurrent.futures

from flask import Flask, jsonify, request, redirect
from google.cloud import bigquery

app = Flask(__name__)
bigquery_client = bigquery.Client()


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

if __name__ == "__main__":
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    app.run(host="127.0.0.1", port=8080, debug=True)
