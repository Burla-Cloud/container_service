import os

from flask import jsonify, Blueprint, request

from container_service import SELF, LOGGER
from container_service.udf_executor import execute_job
from container_service.helpers import ThreadWithExc

BP = Blueprint("endpoints", __name__)
ERROR_ALREADY_LOGGED = False


@BP.get("/")
def get_status():
    """
    There are four possible states:
     1. “READY”: This service is ready to start processing a subjob.
     2. “RUNNING”: This service is processing a subjob.
     3. “FAILED”: This service had an internal error and failed to process the subjob.
     4. “DONE”: This container successfully processed the subjob.
    """
    global ERROR_ALREADY_LOGGED

    thread_is_running = SELF["subjob_thread"] and SELF["subjob_thread"].is_alive()
    error = SELF["subjob_thread"].error if SELF["subjob_thread"] else None
    thread_died = SELF["subjob_thread"] and (not SELF["subjob_thread"].is_alive())

    READY = not SELF["STARTED"]
    RUNNING = thread_is_running and (not error) and (not SELF["DONE"])
    FAILED = error or (thread_died and not SELF["DONE"])
    DONE = SELF["DONE"]

    # raise error if in development so I dont need to go to google cloud logging to see it
    if os.environ.get("IN_DEV") and SELF["subjob_thread"] and SELF["subjob_thread"].error:
        raise SELF["subjob_thread"].error

    if FAILED and not ERROR_ALREADY_LOGGED:
        message = "Subjob Failed" if error else "Subjob died without error!"
        LOGGER.log_struct({"severity": "ERROR", "message": message, "Exception": error})
        ERROR_ALREADY_LOGGED = True

    if READY:
        return jsonify({"status": "READY"})
    elif RUNNING:
        return jsonify({"status": "RUNNING"})
    elif FAILED:
        return jsonify({"status": "FAILED"})
    elif DONE:
        return jsonify({"status": "DONE"})


@BP.post("/jobs/<job_id>")
def start_job(job_id: str):
    # only one job will ever be executed by this service
    if SELF["STARTED"]:  # only one job will ever be executed by this service
        return "STARTED", 409

    # ThreadWithExc is a thread that catches and stores errors.
    # We need so we can save the error until the status of this service is checked.
    thread = ThreadWithExc(target=execute_job, args=(job_id, request.files.get("function_pkl")))
    thread.start()

    SELF["current_job"] = job_id
    SELF["subjob_thread"] = thread
    SELF["STARTED"] = True

    return "Success"
