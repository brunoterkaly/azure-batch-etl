"""Main driver for application logic"""

from azure.storage.filedatalake import DataLakeServiceClient


import os

from etl.config.secrets import _DATALAKE_CONNECTION_STRING


class ADLS2Manager:
    """class"""

    def __init__(self):
        super().__init__()
        self._datalake_service_client = None
        pass

    def connect_adls_gen2(
        self,
    ):

        try:

            self._datalake_service_client = (
                DataLakeServiceClient.from_connection_string(
                    _DATALAKE_CONNECTION_STRING
                )
            )

        except Exception as e:
            print(e)

    def upload_file(self, file_name: str):
        try:

            filesystem = "transformations"
            service_client = (
                self._datalake_service_client
            ) = DataLakeServiceClient.from_connection_string(
                _DATALAKE_CONNECTION_STRING
            )

            # create the file system if it does not exist
            file_system_client = service_client.get_file_system_client(filesystem)
            # print("the file system exists: " + str(file_system_client.exists()))

            if not file_system_client.exists():
                file_system_client.create_file_system()
                # print("the file system is created.")
            # else:
            # print("the file system exists: " + str(file_system_client.exists()))

            data = None
            with open(file_name, "r") as text_file:
                data = text_file.read()
            file_client = file_system_client.get_file_client(
                os.path.basename(file_name)
            )
            file_client.create_file()

            file_client.append_data(data, offset=0, length=len(data))
            file_client.flush_data(len(data))

        except Exception as e:
            print(e)


if __name__ == "__main__":

    """ """
    adls2_manager = ADLS2Manager()
    adls2_manager.connect_adls_gen2()
    file_name = "./file_output/person_transformed.csv"
    adls2_manager.upload_file(file_name)
