import json
import pprint
import sys

from etl.src.data_mappings import DataMappings, ColumnMappings


class JsonManager:
    """Parse JSON data"""

    def __init__(self, mapping_file):
        super().__init__()
        self._json_data = None
        self._mapping_file = mapping_file

    def load_mapping_file(self):
        try:
            with open(self._mapping_file) as f:
                self._json_data = json.load(f)
        except Exception as ex:
            print(ex)

    def show_json(self):
        try:
            print(self._json_data)
        except Exception as ex:
            print(ex)

    def dump_json(self):
        try:
            with open("./file_output/output.json", "w") as json_file:
                json.dump(self._json_data, fp=json_file, sort_keys=True, indent=4)

        except Exception as ex:
            print(ex)

    def show_sections(self, sections):
        try:
            curr_level = self._json_data
            for k in sections:
                curr_level = curr_level[k]
            json.dump(curr_level, fp=sys.stdout, sort_keys=True, indent=4)

        except Exception as ex:
            print(ex)

    def get_section(self, sections):
        try:
            curr_level = self._json_data
            for k in sections:
                curr_level = curr_level[k]
            return curr_level

        except Exception as ex:
            print(ex)


if __name__ == "__main__":
    json_manager = JsonManager(
        "./file_input/person.csv", "./file_input/person_map.json"
    )
    json_manager.load_mapping_file()
    json_manager.show_json()
    json_manager.dump_json()
    json_manager.show_sections(["properties", "activities"])
    inputs = json_manager.get_section(["properties", "activities"])
    input_file = inputs[0]["inputs"][0]["referenceName"]
    output_file = inputs[0]["outputs"][0]["referenceName"]

    json.dump(inputs, fp=sys.stdout, sort_keys=True, indent=4)
    print("[{}]:[INFO] : Done parsing file ...".format(json_manager._mapping_file))
