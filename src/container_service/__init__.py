import os
import sys
import json
import traceback

from flask import Flask, g, request, abort
from google.cloud import logging

# Defined before importing endpoints to prevent cyclic imports
PROJECT_ID = "burla-prod" if os.environ.get("IN_PRODUCTION") == "True" else "burla-test"
JOBS_BUCKET = "burla-jobs-prod" if PROJECT_ID == "burla-prod" else "burla-jobs"

SELF = {
    "STARTED": False,
    "DONE": False,
    "job_id": None,
    "subjob_id": None,
    "subjob_thread": None,
}

from container_service.endpoints import BP as endpoints_bp


app = Flask(__name__)
app.register_blueprint(endpoints_bp)


@app.before_request
def before_request_func():
    g.logger = logging.Client().logger("container_service")


@app.errorhandler(Exception)
def log_exception(exception):
    """
    Logs any exceptions thrown inside a request.
    """
    exc_type, exc_value, exc_traceback = sys.exc_info()
    traceback_details = traceback.format_exception(exc_type, exc_value, exc_traceback)
    traceback_str = "".join(traceback_details)

    try:
        request_json = json.dumps(vars(request))
    except:
        request_json = "Unable to serialize request."

    if exception and os.environ.get("IN_DEV"):
        print(traceback_str, file=sys.stderr)
    elif exception:
        log = {"severity": "ERROR", "exception": traceback_str, "request": request_json}
        g.logger.log_struct(log)

    abort(500)
