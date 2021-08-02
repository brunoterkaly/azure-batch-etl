"""BlobManager: Uploading and downloading blob files"""
from __future__ import print_function
import datetime
import os
from azure.storage.blob import (
    BlobServiceClient,
    BlobClient,
    ContainerClient,
    __version__,
)

class BlobManager:  # pylint: disable=too-few-public-methods
    """Upload and download files from Azure Blob Storage"""

    def __init__(self):
        super().__init__()

    def upload_blob_file(
        self,
        storage_account_connection_string: str,
        container_name: str,
        upload_file: str,
    ):
        """Uploads upload_file to blob storage"""
        try:
            # Create the BlobServiceClient object which will be used to create a container client
            blob_service_client = BlobServiceClient.from_connection_string(
                storage_account_connection_string
            )

            all_containers = blob_service_client.list_containers()
            if not (
                container_name in [container["name"] for container in all_containers]
            ):
                _ = blob_service_client.create_container(container_name)
            else:
                _ = blob_service_client.get_container_client(container_name)

            # Upload the created file
            container_client = ContainerClient.from_connection_string(
                conn_str=storage_account_connection_string,
                container_name=container_name,
            )

            # Upload file
            with open(upload_file, "rb") as data:
                container_client.upload_blob(
                    name=os.path.basename(upload_file), data=data, overwrite=True
                )

        except Exception as e:
            print(e)

        return True

    def download_blob_file(
        self,
        storage_account_connection_string: str,
        container_name: str,
        download_file: str,
    ):
        """Download download_file from blob container"""
        try:
            # Create the BlobServiceClient object which will be used to create a container client
            blob_service_client = BlobServiceClient.from_connection_string(
                storage_account_connection_string
            )
            # blob name is the base filename
            blob_name = os.path.basename(download_file)
            container_client = blob_service_client.get_container_client(container_name)
            blob_list = container_client.list_blobs()
            if not (blob_name in [blob["name"] for blob in blob_list]):
                print(
                    "[{}]:[FAILURE] : Downloading {} ...".format(
                        datetime.datetime.utcnow(), os.path.basename(download_file)
                    )
                )
                return None

            print(
                "[{}]:[INFO] : Downloading {} ...".format(
                    datetime.datetime.utcnow(), os.path.basename(download_file)
                )
            )

            blob = BlobClient.from_connection_string(
                conn_str=storage_account_connection_string,
                container_name=container_name,
                blob_name=blob_name,
            )

            blob_name = download_file
            with open(blob_name, "wb") as my_blob:
                blob_data = blob.download_blob()
                blob_data.readinto(my_blob)

            print(
                "[{}]:[INFO] : download finished. ".format(datetime.datetime.utcnow())
            )
        except Exception as e:
            print(e)
            return None
        return blob_name
