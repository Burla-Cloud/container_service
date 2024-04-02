import pickle
from uuid import uuid4
from time import sleep
from six import reraise

import cloudpickle
from tblib import Traceback
from google.cloud import firestore
from google.cloud.storage import Client, Blob

from env_builder import start_building_environment


def test_healthcheck(client):
    response = client.get("/")
    assert response.json == {"status": "READY"}


def test_everything(client):
    """this is an e2e integration test that relies on live infra"""

    # gcs_client = Client()
    # job_id = str(uuid4()) + "-test"
    # print(f"\nTESTING WITH JOB ID: {job_id}")
    # subjob_id = "3"

    # dependencies = [{"name": "pandas", "version": "2.0.3"}, {"name": "matplotlib"}]

    # def mock_udf(my_input):
    #     import pandas as pd

    #     pd.DataFrame({"hello": ["world", "world", "world", "world"]})
    #     return my_input * 2

    # # upload function
    # pickled_function = cloudpickle.dumps(mock_udf)
    # function_uri = f"gs://burla-jobs/12345/{job_id}/function.pkl"
    # blob = Blob.from_string(function_uri, gcs_client)
    # blob.upload_from_string(data=pickled_function, content_type="application/octet-stream")

    # # upload input
    # mock_input = "hi"
    # pickled_input = cloudpickle.dumps(mock_input)
    # input_uri = f"gs://burla-jobs/{job_id}/inputs/{subjob_id}.pkl"
    # blob = Blob.from_string(input_uri, gcs_client)
    # blob.upload_from_string(data=pickled_input, content_type="application/octet-stream")

    # db = firestore.Client(project="burla-test")
    # job_ref = db.collection("jobs").document(job_id)

    # job_ref.set({"test": True, "function_uri": function_uri})
    # image = "us-docker.pkg.dev/burla-test/burla-subjob-images/default-image:latest"
    # job_ref.update(
    #     {"env.is_copied_from_client": True, "env.packages": dependencies, "env.image": image}
    # )
    # job_ref.collection("sub_jobs").document(subjob_id).set({"claimed": False})

    # start_building_environment(job_id, image=image)

    job_id = "efe4813a-1ff9-4236-b398-dfe10241ebc3"
    gcs_client = Client()
    db = firestore.Client(project="burla-test")
    job_ref = db.collection("jobs").document(job_id)
    #

    response = client.post(f"/jobs/{job_id}")
    assert response.status_code == 200

    # loop until job is done, or env builder fails
    while True:
        sleep(2)

        response = client.get("/")
        assert response.status_code == 200
        response_json = response.get_json()

        if response_json["status"] == "FAILED":
            raise Exception("Executor Failed")

        if response_json["status"] == "DONE":
            break

        env_install_error = job_ref.get().to_dict()["env"].get("install_error")
        if env_install_error:
            raise Exception(env_install_error)

    outputs = []
    for subjob_doc_ref in job_ref.collection("sub_jobs").stream():
        subjob_document = subjob_doc_ref.to_dict()

        if subjob_document.get("udf_error"):
            exception_info = pickle.loads(bytes.fromhex(subjob_document["udf_error"]))
            reraise(
                tp=exception_info["exception_type"],
                value=exception_info["exception"],
                tb=Traceback.from_dict(exception_info["traceback_dict"]).as_traceback(),
            )

        output_uri = f"gs://burla-jobs/{job_id}/outputs/{subjob_doc_ref.id}.pkl"
        output = cloudpickle.loads(Blob.from_string(output_uri, gcs_client).download_as_bytes())
        outputs.append(output)

    print(f"OUTPUTS: {outputs}")
