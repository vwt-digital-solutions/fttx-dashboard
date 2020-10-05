from google.cloud import secretmanager_v1
import logging


def get_secret(project_id, secret_id):
    """
    Returns a Secret Manager secret.
    """
    logging.info(f"Getting secret for project {project_id}, secret id {secret_id}")
    client = secretmanager_v1.SecretManagerServiceClient()

    secret_name = client.secret_version_path(
        project_id,
        secret_id,
        'latest')

    response = client.access_secret_version(secret_name)
    payload = response.payload.data.decode('utf-8')
    logging.info("Returning secret")
    return payload
