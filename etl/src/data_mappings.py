"""Mapping source and destination data files"""


class ColumnMappings:
    def __init__(
        self,
        source_column_name: str,
        source_column_type: str,
        source_column_physical_type: str,
        destination_column_name: str,
        destination_column_type: str,
        destination_column_physical_type: str,
    ):
        self._source_column_name = source_column_name
        self._source_column_type = source_column_type
        self._source_column_physical_type = source_column_physical_type

        self._destination_column_name = destination_column_name
        self._destination_column_type = destination_column_type
        self._destination_column_physical_type = destination_column_physical_type


class DataMappings:
    def __init__(
        self,
        source_data_filename: str,
        destination_data_filename: str,
    ):
        self._source_data_filename = source_data_filename
        self._destination_data_filename = destination_data_filename
        self._column_mappings = []
