import sys
import pickle
import tarfile
from time import time, sleep
from pathlib import Path
from typing import Optional, Union

import cloudpickle
from tblib import Traceback
from google.cloud import firestore
from google.cloud.firestore import FieldFilter
from google.cloud.storage import Client as StorageClient, Blob
from google.cloud.firestore_v1 import ArrayUnion, CollectionReference

from container_service import JOBS_BUCKET, SELF

from google.cloud import logging


class EmptyInputQueue(Exception):
    pass


class InputGetter:
    """
    The policy implemented here is designed to minimize firestore document collisions.
    If too many workers try to grab a firestore document (input) at the same time, stuff breaks.
    """

    def __init__(
        self,
        db: firestore.Client,
        inputs_id: str,
        num_inputs: int,
        starting_index: int,
        current_parallelism: int,
    ):
        self.db = db
        self.inputs_id = inputs_id
        self.num_inputs = num_inputs
        self.current_parallelism = current_parallelism
        self.starting_index = starting_index

        self.current_index = starting_index
        self.getting_first_input = True

        inputs_parent_doc = self.db.collection("inputs").document(self.inputs_id)
        self.inputs_collection_ref = inputs_parent_doc.collection("inputs")

        self.logger = logging.Client().logger(f"WORKER_index_{starting_index}")

    def __empty_input_queue(self):
        filter = FieldFilter("claimed", "==", False)
        unclaimed_inputs_query = self.inputs_collection_ref.where(filter=filter)
        n_unclaimed_inputs = unclaimed_inputs_query.count().get()[0][0].value
        return n_unclaimed_inputs == 0

    def __attempt_to_claim_input(self, input_index: int) -> Union[None, bytes]:
        @firestore.transactional
        def attempt_to_claim_input(transaction: firestore.Transaction, input_index: int):
            document_reference = self.inputs_collection_ref.document(str(input_index))
            document_snapshot = document_reference.get(transaction=transaction)
            if document_snapshot.exists and not document_snapshot.get("claimed"):
                transaction.update(document_reference, {"claimed": True})
                return document_snapshot.get("input")

        try:
            transaction = self.db.transaction(max_attempts=1)
            return attempt_to_claim_input(transaction, input_index)
        except ValueError as e:
            if "Failed to commit transaction" in str(e):
                raise ValueError(f"DOCUMENT CONTENTION AT INDEX: {input_index}")
            else:
                raise e

    def get_next_input(self):
        # avoid checking self.__empty_input_queue() on first input to reduce latency.
        if self.getting_first_input:
            self.getting_first_input = False
        elif self.__empty_input_queue():
            raise EmptyInputQueue()

        struct = dict(
            message=f"Attempting to claim input at index: {self.current_index}",
            current_index=self.current_index,
            current_parallelism=self.current_parallelism,
            num_inputs=self.num_inputs,
        )
        self.logger.log_struct(struct)

        input_pkl = self.__attempt_to_claim_input(self.current_index)
        self.current_index = (self.current_index + self.current_parallelism) % self.num_inputs

        if input_pkl:
            return self.current_index, input_pkl
        else:
            return self.get_next_input()


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
            type=exception_type,
            exception=exception,
            traceback_dict=Traceback(traceback).to_dict(),
        )
    )
    return pickled_exception_info


def execute_job(job_id: str, starting_index: int, function_pkl: Optional[bytes] = None):
    function_in_gcs = function_pkl is None
    gcs_client = None

    db = firestore.Client()
    job_ref = db.collection("jobs").document(job_id)
    results_collection_ref = job_ref.collection("results")
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

    input_getter = InputGetter(
        db,
        inputs_id=job["inputs_id"],
        num_inputs=job["n_inputs"],
        starting_index=starting_index,
        current_parallelism=job["planned_future_job_parallelism"],
    )

    while True:
        try:
            input_index, input_pkl = input_getter.get_next_input()
        except EmptyInputQueue:
            SELF["DONE"] = True
            return

        exec_info = None
        with _FirestoreLogger(logs_collection_ref, input_index):
            try:
                input_ = cloudpickle.loads(input_pkl)
                return_value = user_defined_function(input_)
            except Exception:
                exec_info = sys.exc_info()

        result_pkl = _serialize_error(exec_info) if exec_info else cloudpickle.dumps(return_value)
        result_too_big = len(result_pkl) > 1_048_376

        if result_too_big:
            noun = "Error" if exec_info else "Return value"
            msg = f"{noun} from input at index {input_index} is greater than 1MB in size."
            raise Exception(f"{msg}\nUnable to store result.")

        result_doc = {"is_error": bool(exec_info), "result_pkl": result_pkl}
        results_collection_ref.document(str(input_index)).set(result_doc)
