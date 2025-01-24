import json
import re

TPC_DI_NOTEBOOK_PATH = './run-smaller-vm-2e/tpc-di-setup.ipynb'
LOAD_CELL_ID = 18
DBT_RUN_CELL_ID = 4

def get_sum_run_time(table_creation_times_dict):
    sum_run_time = 0
    for db, time in table_creation_times_dict.items():
        sum_run_time += time
    return sum_run_time

def save_dicts_to_csv(dict1, dict2, dict5):
    csv_string = ''
    # TODO
    # for key in dict1.keys():


with open(TPC_DI_NOTEBOOK_PATH, 'r') as file:
    data = file.read()

json_data = json.loads(data)
dbt_run_lines = json_data["cells"][LOAD_CELL_ID]["outputs"][DBT_RUN_CELL_ID]['text']

table_creation_times_dict = dict()

for line in dbt_run_lines:
    if ('of' in line and 'OK' in line and 'WARN' not in line):
        # print(line)
        table_name_result = re.search("model (.*) \.", line)
        # print(table_name_result.group(1))
        table_name = table_name_result.group(1)

        time_result = re.search("in (.*)s]", line)
        # print(time_result)
        # print(time_result.group(1))
        time = time_result.group(1)

        table_creation_times_dict[table_name] = float(time)

print(table_creation_times_dict)
print(get_sum_run_time(table_creation_times_dict))
