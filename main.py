import concurrent.futures

from flask import Flask, jsonify, request
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

@app.route("/rest/user", methods=["GET","POST","DELETE"])
def user():
    if request.method == "GET":
        patient_id = request.args.get('patient_id', type=int)

        return "Hello " + str(patient_id)

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
