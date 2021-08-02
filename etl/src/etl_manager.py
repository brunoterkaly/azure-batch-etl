"""Main driver for application logic"""
from __future__ import print_function
import petl
import sys
from petl import fromcsv

from etl.src.blob_manager import BlobManager
from etl.src.data_mappings import ColumnMappings, DataMappings
from etl.src.db_manager import DBManager
from etl.src.json_manager import JsonManager
from etl.config.general import _INPUT_FILE, _CONTAINER_NAME
from etl.config.secrets import _STORAGE_CONNECTION_STRING

from etl.src.adls2_manager import ADLS2Manager


class EtlManager:
    """Main orchestrating class"""

    def __init__(self):
        super().__init__()
        pass

    def download_blob_file(
        self,
        storage_account_connection_string: str,
        container_name: str,
        download_file: str,
    ) -> str:
        """Download from blob storage"""
        blob_manager = BlobManager()
        input_file = blob_manager.download_blob_file(
            storage_account_connection_string, container_name, download_file
        )
        return input_file

    def upload_blob_file(
        self,
        storage_account_connection_string: str,
        container_name: str,
        upload_file: str,
    ) -> str:
        """Download from blob storage"""
        blob_manager = BlobManager()
        input_file = blob_manager.upload_blob_file(
            storage_account_connection_string, container_name, upload_file
        )
        return input_file

    def parse_mapping_file(self, mapping_file: str):

        try:

            json_manager = JsonManager(mapping_file)
            json_manager.load_mapping_file()
            inputs = json_manager.get_section(["properties", "activities"])
            source_file = inputs[0]["inputs"][0]["referenceName"]
            destination_file = inputs[0]["outputs"][0]["referenceName"]
            data_mappings = DataMappings(source_file, destination_file)
            # Navigate to the mapping section, still have json.
            mappings_array = inputs[0]["typeProperties"]["translator"]["mappings"]
            for i in range(0, len(mappings_array)):
                column_mappings = ColumnMappings(
                    mappings_array[i]["source"]["name"],
                    mappings_array[i]["source"]["type"],
                    mappings_array[i]["source"]["physicalType"],
                    mappings_array[i]["sink"]["name"],
                    mappings_array[i]["sink"]["type"],
                    mappings_array[i]["sink"]["physicalType"],
                )
                data_mappings._column_mappings.append(column_mappings)
            return data_mappings
        except Exception as ex:
            print(ex)
        return None

    # bt
    def transform_data(self, data_mappings: DataMappings):

        # Adjust path to read from files_input
        source_data_filename = ".//file_input//{}".format(
            data_mappings._source_data_filename
        )
        try:
            # Open source data file and load
            person_table = fromcsv(source_data_filename)
            columns_to_cut = []
            # Loop through source columns to know what to cut
            for c in data_mappings._column_mappings:
                columns_to_cut.append(c._source_column_name)
            person_table = petl.cut(person_table, *columns_to_cut)
            new_column_names = []
            # Loop through destination columns to rename them
            for c in data_mappings._column_mappings:
                new_column_names.append(c._destination_column_name)

            # Do the renaming of columns
            for i in range(0, len(columns_to_cut)):
                person_table = petl.rename(
                    person_table, columns_to_cut[i], new_column_names[i]
                )
            petl.tocsv(
                person_table,
                ".//file_output//{}".format(data_mappings._destination_data_filename),
            )

        except Exception as ex:
            print(ex)

    def transform_and_load(self, input_file: str):

        try:
            db_manager = DBManager()
            conn = db_manager.connect_to_db()
            cursor = conn.cursor()
            db_manager.create_table(conn.cursor())
            db_manager.insert_data(conn, cursor, input_file)
            conn.commit()
            db_manager.show_data(
                cursor,
                "SELECT * FROM persons WHERE bodysite_code={}".format("51185008"),
            )

            conn.close()
        except Exception as ex:
            print(ex)


if __name__ == "__main__":
    """
    The EtlManager will download a blob and then process it
    """

    input_file = ".//file_input//{}".format(sys.argv[1])
    mapping_file = ".//file_input//{}".format(sys.argv[2])

    try:
        etl_manager = EtlManager()

        # Download person.csv and person_map.csv
        input_file = etl_manager.download_blob_file(
            _STORAGE_CONNECTION_STRING, _CONTAINER_NAME, input_file
        )
        mapping_file = etl_manager.download_blob_file(
            _STORAGE_CONNECTION_STRING, _CONTAINER_NAME, mapping_file
        )

        if input_file is None or mapping_file is None:
            print(
                "[Failed file {}, Container {}]:[FAILURE] : Unable to download. ".format(
                    _INPUT_FILE, _CONTAINER_NAME
                )
            )
            exit()

        data_mappings = etl_manager.parse_mapping_file(mapping_file)
        etl_manager.transform_data(data_mappings)

        output_file = ".//file_output//{}".format("person_transformed.csv")
        result = etl_manager.upload_blob_file(
            _STORAGE_CONNECTION_STRING, "output", output_file
        )

        adls2_manager = ADLS2Manager()
        adls2_manager.connect_adls_gen2()
        file_name = "./file_output/person_transformed.csv"
        adls2_manager.upload_file(file_name)

        print("[{}]:[INFO] : Transformed file uploaded... ".format(output_file))
    except Exception as e:
        print(e)


