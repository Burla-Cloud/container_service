import sys
import pickle
import tarfile
from time import time, sleep
from pathlib import Path

import cloudpickle
from tblib import Traceback
from google.cloud import firestore
from google.cloud.storage import Client as StorageClient, Blob

from container_service import JOBS_BUCKET, SELF


class _FirestoreLogger:
    def __init__(self, job_document_ref, subjob_id):
        self.job_document_ref = job_document_ref
        self.subjob_id = subjob_id

    def write(self, message):
        if message.strip():
            log = {
                "epoch": int(time()),
                "text": message.strip(),
                "sub_job_id": self.subjob_id,
            }
            self.job_document_ref.collection("logs").add(log)
        self.original_stdout.write(message)

    def flush(self):
        self.original_stdout.flush()

    def __enter__(self):
        self.original_stdout = sys.stdout
        sys.stdout = self

    def __exit__(self, exc_type, exc_value, traceback):
        sys.stdout = self.original_stdout


def _install_copied_environment(gcs_client, job, job_document_ref, retries=0):
    job_env = job_document_ref.get().to_dict()["env"]

    if retries == 600:
        raise Exception("Timed out waiting for environment to finished building after 1H")

    if not (job_env.get("uri") or job_env.get("install_error")):
        sleep(6)
        _install_copied_environment(gcs_client, job, job_document_ref, retries=retries + 1)

    if job_env.get("uri"):
        extra_packages_path = Path("/extra_packages")
        extra_packages_path.mkdir(exist_ok=True)
        sys.path.append(str(extra_packages_path))

        blob = Blob.from_string(job_env["uri"], gcs_client)
        blob.download_to_filename(extra_packages_path / "env.tar.gz")
        with tarfile.open(extra_packages_path / "env.tar.gz", "r:gz") as tar_ref:
            tar_ref.extractall(path=extra_packages_path)

    # The error is reported by the env builder, so we should silently fail here.
    if job.get("install_error"):
        sys.exit(0)


def _serialize_error(exc_info):
    # exc_info is tuple returned by sys.exc_info()
    exception_type, exception, traceback = exc_info
    pickled_exception_info = pickle.dumps(
        dict(
            exception_type=exception_type,
            exception=exception,
            traceback_dict=Traceback(traceback).to_dict(),
        )
    )
    return pickled_exception_info.hex()


def get_next_subjob(db, job_document_ref):
    #
    @firestore.transactional
    def attempt_to_claim_subjob(transaction):
        sub_jobs_ref = job_document_ref.collection("sub_jobs")
        filter = firestore.FieldFilter("claimed", "==", False)
        query = sub_jobs_ref.where(filter=filter).limit(1)  # TODO: randomly order before picking
        docs = list(query.stream())
        if len(docs) != 0:
            doc_ref = sub_jobs_ref.document(docs[0].id)
            snapshot = doc_ref.get(transaction=transaction)
            if snapshot.exists and not snapshot.get("claimed"):
                transaction.update(doc_ref, {"claimed": True})
                return doc_ref

    transaction = db.transaction(max_attempts=10)
    return attempt_to_claim_subjob(transaction)


def execute_job(job_id: str):
    gcs_client = StorageClient()
    db = firestore.Client()
    job_document_ref = db.collection("jobs").document(job_id)
    job = job_document_ref.get().to_dict()

    pickled_function = Blob.from_string(job["function_uri"], gcs_client).download_as_bytes()
    user_defined_function = cloudpickle.loads(pickled_function)

    if job.get("env", {}).get("is_copied_from_client"):
        _install_copied_environment(gcs_client, job, job_document_ref)

    subjob_document_ref = get_next_subjob(db, job_document_ref)
    while subjob_document_ref is not None:
        input_uri = f"gs://{JOBS_BUCKET}/{job_id}/inputs/{subjob_document_ref.id}.pkl"
        input_ = cloudpickle.loads(Blob.from_string(input_uri, gcs_client).download_as_bytes())

        udf_error = False
        with _FirestoreLogger(job_document_ref, subjob_document_ref.id):
            subjob_document_ref.update({"udf_started": True, "udf_started_at": time()})
            try:
                return_value = user_defined_function(input_)
            except Exception:
                subjob_document_ref.update({"udf_error": _serialize_error(sys.exc_info())})
                udf_error = True

        if not udf_error:
            return_value_pkl = cloudpickle.dumps(return_value)
            output_uri = f"gs://{JOBS_BUCKET}/{job_id}/outputs/{subjob_document_ref.id}.pkl"
            blob = Blob.from_string(output_uri, gcs_client)
            blob.upload_from_string(data=return_value_pkl, content_type="application/octet-stream")
            subjob_document_ref.update({"done": True})

        subjob_document_ref = get_next_subjob(db, job_document_ref)

    SELF["DONE"] = True
