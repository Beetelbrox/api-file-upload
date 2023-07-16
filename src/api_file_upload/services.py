import datetime as dt

from google import auth
from google.auth.transport import requests
from google.cloud.storage.blob import Blob


def refresh_token() -> str:
    credentials, _ = auth.default()
    if credentials.token is None:
        # Perform a refresh request to populate the access token of the
        # current credentials.
        credentials.refresh(requests.Request())
    return credentials.token


# For creating the pre-signed URL for GCS:
# https://medium.com/@evanpeterson17/how-to-generate-signed-urls-using-python-in-google-cloud-run-835ddad5366
# We need a service account with `roles/iam.serviceAccountTokenCreator` and whatever is needed to read the file,
# in our case `storage.objects.get` on the bucket
# It seems that with the workaround below you need `roles/iam.serviceAccountTokenCreator` on both the service account
# AND the one sending the request. Even if the former has owner on the workspace
def create_signed_url(blob: Blob, service_account_email: str, token: str) -> str:
    return blob.generate_signed_url(
        version="v4",
        expiration=dt.timedelta(minutes=15),
        service_account_email=service_account_email,
        access_token=token,
        method="GET",
    )
