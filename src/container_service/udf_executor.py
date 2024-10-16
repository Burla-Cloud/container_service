import sys
import pickle
import tarfile
from time import time, sleep
from pathlib import Path
from typing import Optional

import cloudpickle
from tblib import Traceback
from google.cloud import firestore
from google.cloud.storage import Client as StorageClient, Blob
from google.cloud.firestore_v1 import ArrayUnion, CollectionReference

from container_service import JOBS_BUCKET, SELF


class EmptyInputQueue(Exception):
    pass


<<<<<<< Updated upstream
=======
class InputGetter:
    """
    The policy implemented here is designed to minimize firestore document collisions.
    If too many workers try to grab a firestore document (input) at the same time, stuff breaks.
    """

    def __init__(
        self,
        db_headers: dict,
        inputs_id: str,
        num_inputs: int,
        starting_index: int,
        parallelism: int,
    ):
        self.db_headers = db_headers
        self.inputs_id = inputs_id
        self.num_inputs = num_inputs
        self.parallelism = parallelism
        self.starting_index = starting_index
        self.current_index = starting_index

    def __attempt_to_claim_input(self, input_index: int) -> Union[None, bytes]:
        batch_size = 100
        batch_min_index = (input_index // batch_size) * batch_size
        collection_name = f"{batch_min_index}-{batch_min_index + batch_size}"
        SELF["WORKER_LOGS"].append(f"attempting to claim input {input_index}")

        # grab doc
        input_pkl = None
        input_doc_url = f"{DB_BASE_URL}/inputs/{self.inputs_id}/{collection_name}/{input_index}"
        for i in range(10):
            response = requests.get(input_doc_url, headers=self.db_headers)
            if response.status_code == 200:
                input_pkl = base64.b64decode(response.json()["fields"]["input"]["bytesValue"])
                is_claimed = response.json()["fields"]["claimed"]["booleanValue"]
                break
            if response.status_code != 404:
                raise Exception(f"non 404/200 response trying to get input {input_index}?")
            sleep(i * i * 0.1)  # 0.0, 0.1, 0.4, 0.9, 1.6, 2.5, 3.6, 4.9, 6.4, 8.1 ...

        if not input_pkl:
            raise Exception(f"DOCUMENT #{input_index} NOT FOUND after 10 attempts.")
        elif is_claimed:
            return None

        # mark doc as claimed
        SELF["WORKER_LOGS"].append(f"input found, marking input {input_index} as claimed")
        update_claimed_url = f"{input_doc_url}?updateMask.fieldPaths=claimed"
        data = {"fields": {"claimed": {"booleanValue": True}}}
        response = requests.patch(update_claimed_url, headers=self.db_headers, json=data)
        response.raise_for_status()
        SELF["WORKER_LOGS"].append(f"successfully marked input {input_index} as claimed")
        return input_pkl

    def __get_next_index(self, old_index: int):
        new_index = (old_index + self.parallelism) % self.num_inputs
        if (new_index == self.starting_index) or (new_index == old_index):
            new_index = (new_index + 1) % self.num_inputs
            self.starting_index += 1
        SELF["WORKER_LOGS"].append(f"computed new index: old={old_index}, new={new_index}")
        return new_index

    def get_next_input(self, attempt=0):

        input_pkl = self.__attempt_to_claim_input(self.current_index)
        input_index = self.current_index
        self.current_index = self.__get_next_index(old_index=self.current_index)

        if input_pkl:
            return input_index, input_pkl
        elif attempt == 5:
            # If I try to claim 5 documents and they are all claimed then every document was
            # checked 5 times and this worker can DEFINITELY stop trying! ?
            # (or another worker is 5 docs ahead of this one and covering for it / will continue to)
            raise EmptyInputQueue()
        else:
            return self.get_next_input(attempt=attempt + 1)


>>>>>>> Stashed changes
class _FirestoreLogger:

    def __init__(self, logs_collection_ref: CollectionReference, input_index: int):
        self.logs_collection_ref = logs_collection_ref
        self.input_index = input_index

    def write(self, msg):
        self.original_stdout.write(msg)
        if msg.strip() and (len(msg.encode("utf-8")) > 1_048_376):  # (1mb - est overhead):
            msg_truncated = msg.encode("utf-8")[:1_048_376].decode("utf-8", errors="ignore")
            msg = msg_truncated + "<too-long--remaining-msg-truncated-due-to-length>"
        if msg.strip():
            log = {"ts": time(), "input_index": self.input_index, "msg": msg}
            self.logs_collection_ref.add(log)

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


def get_next_input(db: firestore.Client, inputs_id: str):
    @firestore.transactional
    def attempt_to_claim_subjob(transaction):
        inputs_ref = db.collection("inputs").document(inputs_id).collection("inputs")
        filter = firestore.FieldFilter("claimed", "==", False)
        query = inputs_ref.where(filter=filter).limit(1)  # TODO: randomly order before picking
        docs = list(query.stream())
        if len(docs) != 0:
            doc_ref = inputs_ref.document(docs[0].id)
            snapshot = doc_ref.get(transaction=transaction)
            if snapshot.exists and not snapshot.get("claimed"):
                transaction.update(doc_ref, {"claimed": True})
                return docs[0].id, snapshot.get("input")

    transaction = db.transaction(max_attempts=10)
    input_pkl = attempt_to_claim_subjob(transaction)
    if input_pkl:
        return input_pkl
    else:
        raise EmptyInputQueue()


def execute_job(job_id: str, function_pkl: Optional[bytes] = None):
    function_in_gcs = function_pkl is None
    gcs_client = None

    db = firestore.Client()
    job_ref = db.collection("jobs").document(job_id)
    outputs_collection_ref = job_ref.collection("outputs")
    logs_collection_ref = job_ref.collection("logs")
    job = job_ref.get().to_dict()

    if function_in_gcs:
        gcs_client = StorageClient() if not gcs_client else gcs_client
        function_uri = f"gs://{JOBS_BUCKET}/{job_id}/function.pkl"
        pickled_function_blob = Blob.from_string(function_uri, gcs_client)

        start_time = time()
        timeout_sec = 10
        while not pickled_function_blob.exists():
            if time() - start_time > timeout_sec:
                raise Exception(f"{function_uri} did not appear in under {timeout_sec} sec.")
            sleep(0.1)

        pickled_function = pickled_function_blob.download_as_bytes()
        user_defined_function = cloudpickle.loads(pickled_function)
    else:
        user_defined_function = cloudpickle.loads(function_pkl)

    if job.get("env", {}).get("is_copied_from_client"):
        gcs_client = StorageClient() if not gcs_client else gcs_client
        _install_copied_environment(gcs_client, job, job_ref)

    while True:
        try:
            input_index, input_pkl = get_next_input(db, job["inputs_id"])
        except EmptyInputQueue:
            SELF["DONE"] = True
            return

        udf_error = False
        with _FirestoreLogger(logs_collection_ref, input_index):
            try:
                input_ = cloudpickle.loads(input_pkl)
                return_value = user_defined_function(input_)
            except Exception:
                udf_error = _serialize_error(sys.exc_info())
                udf_error_with_id = {"input_index": input_index, "udf_error": udf_error}
                job_ref.update({"udf_errors": ArrayUnion([udf_error_with_id])})
                udf_error = True

        if not udf_error:
            output_pkl = cloudpickle.dumps(return_value)
            output_too_big = len(output_pkl) > 1_048_376
            if output_too_big:
                msg = f"Input at index {input_index} is greater than 1MB in size.\n"
                msg += "Inputs greater than 1MB are unfortunately not yet supported."
                raise Exception(msg)
            else:
                outputs_collection_ref.document(str(input_index)).set({"output": output_pkl})
