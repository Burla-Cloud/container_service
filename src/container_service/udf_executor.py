import sys
import pickle
import tarfile
import struct
from time import time, sleep
from pathlib import Path
from typing import Optional

import cloudpickle
from tblib import Traceback
from google.cloud import firestore
from google.cloud import pubsub
from google.cloud.storage import Client as StorageClient, Blob
from google.cloud.pubsub_v1.types import BatchSettings
from google.cloud.firestore_v1 import ArrayUnion

from container_service import (
    JOBS_BUCKET,
    SELF,
    INPUTS_SUBSCRIPTION_PATH,
    OUTPUTS_TOPIC_PATH,
    LOGS_TOPIC_PATH,
)

batch_settings = BatchSettings(max_bytes=10000000, max_latency=0, max_messages=1)
OUTPUTS_PUBLISHER = pubsub.PublisherClient(batch_settings=batch_settings)

batch_settings = BatchSettings(max_bytes=10000000, max_latency=0.1, max_messages=1000)
LOGS_PUBLISHER = pubsub.PublisherClient(batch_settings=batch_settings)

INPUTS_SUBSCRIBER = pubsub.SubscriberClient()


class EmptyPubSubTopic(Exception):
    pass


class _PubSubLogger:

    def write(self, message):
        if message.strip():
            data = message.encode("utf-8")
            LOGS_PUBLISHER.publish(topic=LOGS_TOPIC_PATH, data=data)
            # , ordering_key=LOGS_TOPIC_PATH)
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


def get_next_input(pubsub_subscriber: pubsub.SubscriberClient, attempt=0):
    response = pubsub_subscriber.pull(subscription=INPUTS_SUBSCRIPTION_PATH, max_messages=1)
    messages = response.received_messages

    if messages:
        ack_id = messages[0].ack_id
        pubsub_subscriber.acknowledge(subscription=INPUTS_SUBSCRIPTION_PATH, ack_ids=[ack_id])
        packed_data = messages[0].message.data
        input_index = int.from_bytes(packed_data[:4], byteorder="big")
        return input_index, cloudpickle.loads(packed_data[4:])
    elif attempt == 3:
        raise EmptyPubSubTopic("No inputs found in Topic.")
    else:
        sleep(0.5)
        return get_next_input(pubsub_subscriber, attempt=attempt + 1)


def execute_job(job_id: str, function_pkl: Optional[bytes] = None):
    function_in_gcs = function_pkl is None
    gcs_client = None

    db = firestore.Client()
    job_document_ref = db.collection("jobs").document(job_id)
    job = job_document_ref.get().to_dict()

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
        _install_copied_environment(gcs_client, job, job_document_ref)

    while True:
        try:
            input_index, input_ = get_next_input(INPUTS_SUBSCRIBER)
        except EmptyPubSubTopic:
            SELF["DONE"] = True
            return

        udf_error = False
        with _PubSubLogger():
            try:
                return_value = user_defined_function(input_)
            except Exception:
                udf_error = _serialize_error(sys.exc_info())
                udf_error_with_id = {"input_index": input_index, "udf_error": udf_error}
                job_document_ref.update({"udf_errors": ArrayUnion([udf_error_with_id])})
                udf_error = True

        if not udf_error:
            output_pkl = cloudpickle.dumps(return_value)
            OUTPUTS_PUBLISHER.publish(topic=OUTPUTS_TOPIC_PATH, data=output_pkl)
