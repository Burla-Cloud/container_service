import threading

from google.cloud.secretmanager import SecretManagerServiceClient

from container_service import PROJECT_ID


def get_secret(secret_name: str):
    client = SecretManagerServiceClient()
    secret_path = client.secret_version_path(PROJECT_ID, secret_name, "latest")
    response = client.access_secret_version(request={"name": secret_path})
    return response.payload.data.decode("UTF-8")


class ThreadWithExc(threading.Thread):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.error = None

    def run(self):
        try:
            if self._target:
                self._target(*self._args, **self._kwargs)
        except Exception as e:
            self.error = e
