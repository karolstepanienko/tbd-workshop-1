import json
import re
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator

TPC_DI_NOTEBOOK_PATH_2n_1e = './e2-standard-4/2n/1e/tpc-di-setup.ipynb'

TPC_DI_NOTEBOOK_PATH_2n_2e = './e2-standard-4/2n/1e/tpc-di-setup.ipynb'
TPC_DI_NOTEBOOK_PATH_2n_5e = './e2-standard-4/2n/1e/tpc-di-setup.ipynb'
# TODO switch when tests ready
# TPC_DI_NOTEBOOK_PATH_2n_2e = './e2-standard-4/2n/2e/tpc-di-setup.ipynb'
# TPC_DI_NOTEBOOK_PATH_2n_5e = './e2-standard-4/2n/5e/tpc-di-setup.ipynb'

TPC_DI_NOTEBOOK_PATH_5n_5e = './e2-standard-4/5n/5e/tpc-di-setup.ipynb'

GRAPH_TARGET_PATH = '../doc/figures/phase2/'

LOAD_CELL_ID = 18
DBT_RUN_CELL_ID = 4

def get_table_creation_times_dict(path):
    with open(path, 'r') as file:
        data = file.read()

    json_data = json.loads(data)
    dbt_run_lines = json_data["cells"][LOAD_CELL_ID]["outputs"][DBT_RUN_CELL_ID]['text']
    # print(dbt_run_lines)

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
    return table_creation_times_dict

def get_sum_run_time(table_creation_times_dict):
    sum_run_time = 0
    for db, time in table_creation_times_dict.items():
        sum_run_time += time
    return round(sum_run_time, 2)

def save_dicts_to_csv(dict_2n_e1, dict_2n_e2, dict_2n_e5, dict_5n_e5):
    csv_string = 'database;2n_e1;2n_e2;2n_e5;5n_e5\n'

    for key in dict_2n_e1.keys():
        csv_string += key + ';' \
            + str(dict_2n_e1[key]) + ';' \
            + str(dict_2n_e2[key]) + ';' \
            + str(dict_2n_e5[key]) + ';' \
            + str(dict_5n_e5[key]) + '\n'

    # print(csv_string)
    file = open('output.csv', 'w')
    file.write(csv_string)
    file.close()

def print_markdown_table(dict_2n_e1, dict_2n_e2, dict_2n_e5, dict_5n_e5):
    assert(dict_2n_e1.keys() == dict_2n_e2.keys() == dict_2n_e5.keys() == dict_5n_e5.keys())

    table_string =  "| db.table | 2n_e1 | 2n_e2 | 2n_e5 | 5n_e5 |\n"
    table_string += "| -------- | ----- | ----- | ----- | ----- |\n"
    for key in dict_2n_e1.keys():
        table_string += key + " | " \
            + str(dict_2n_e1[key]) + " | " \
            + str(dict_2n_e2[key]) + " | " \
            + str(dict_2n_e5[key]) + " | " \
            + str(dict_5n_e5[key]) + " |\n"
    print(table_string)

def print_times(path):
    times = get_table_creation_times_dict(path)
    print(times)
    print(get_sum_run_time(times))

def plot_total_time(dict_2n_e1, dict_2n_e2, dict_2n_e5):
    fg = plt.figure()
    ax = fg.gca()
    ax.set_title("Total time of 'dbt run'")
    x = [1, 2, 5]
    y = [get_sum_run_time(dict_2n_e1), get_sum_run_time(dict_2n_e2), get_sum_run_time(dict_2n_e5)]
    ax.set_ylabel('Time [s]')
    ax.set_xlabel('Number of executors')
    ax.xaxis.set_major_locator(MaxNLocator(integer=True))
    ax.grid()
    ax.plot(x, y)
    fg.savefig(GRAPH_TARGET_PATH + "total_dbt_run_times.png")

def plot_times(dict_2n_e1, dict_2n_e2, dict_2n_e5):
    bar_height = 0.2
    fg = plt.figure(figsize=(12, 20))
    fg.subplots_adjust(left=0.3, bottom=0.04, top=0.96)
    ax = fg.gca()
    ax.set_title("Time of 'dbt run' for each table and executor number")

    br1 = []
    for i in range(len(dict_2n_e1.keys())):
        br1.append(i)
    br2 = [x + bar_height for x in br1]
    br3 = [x + bar_height for x in br2]

    ax.barh(br1, dict_2n_e1.values(), color ='r', height=bar_height, edgecolor ='grey', label ='2n_e1')
    ax.barh(br2, dict_2n_e2.values(), color ='g', height=bar_height, edgecolor ='grey', label ='2n_e2')
    ax.barh(br3, dict_2n_e5.values(), color ='b', height=bar_height, edgecolor ='grey', label ='2n_e5')

    ax.set_xlabel('Time [s]')
    ax.set_yticks([r + bar_height for r in range(len(dict_2n_e1.keys()))], dict_2n_e1.keys())

    colors = {'1':'red', '2':'green', '5': 'blue'}
    labels = list(colors.keys())
    handles = [plt.Rectangle((0,0),1,1, color=colors[label]) for label in labels]
    ax.legend(handles, labels)

    fg.savefig(GRAPH_TARGET_PATH + 'all_dbt_run_times.png')

""" Plots with nodes """

def add_labels(ax, x, y):
    for i in range(len(x)):
        ax.text(i, y[i], y[i], ha = 'center')

def plot_total_time_with_nodes(dict_2n_e1, dict_2n_e2, dict_2n_e5, dict_5n_e5):
    fg = plt.figure()
    ax = fg.gca()
    ax.set_title("Total time of 'dbt run' for different number of nodes")

    labels = ['2n_e1', '2n_e2', '2n_e5', '5n_e5']
    values = [get_sum_run_time(dict_2n_e1), get_sum_run_time(dict_2n_e2), get_sum_run_time(dict_2n_e5), get_sum_run_time(dict_5n_e5)]
    ax.bar(labels, values)

    add_labels(ax, labels, values)

    ax.set_ylabel('Time [s]')
    ax.set_xlabel('<number of nodes>n_e<number of executors>')
    ax.xaxis.set_major_locator(MaxNLocator(integer=True))
    fg.savefig(GRAPH_TARGET_PATH + "total_dbt_run_times_with_nodes.png")

def plot_times_with_nodes(dict_2n_e1, dict_2n_e2, dict_2n_e5, dict_5n_e5):
    bar_height = 0.2
    fg = plt.figure(figsize=(12, 20))
    fg.subplots_adjust(left=0.3, bottom=0.04, top=0.96)
    ax = fg.gca()
    ax.set_title("Time of 'dbt run' for each table, node and executor number")

    br1 = []
    for i in range(len(dict_2n_e1.keys())):
        br1.append(i)
    br2 = [x + bar_height for x in br1]
    br3 = [x + bar_height for x in br2]
    br4 = [x + bar_height for x in br3]

    ax.barh(br1, dict_2n_e1.values(), color ='r', height=bar_height, edgecolor ='grey', label ='2n_e1')
    ax.barh(br2, dict_2n_e2.values(), color ='g', height=bar_height, edgecolor ='grey', label ='2n_e2')
    ax.barh(br3, dict_2n_e5.values(), color ='b', height=bar_height, edgecolor ='grey', label ='2n_e5')
    ax.barh(br4, dict_5n_e5.values(), color ='black', height=bar_height, edgecolor ='grey', label ='5n_e5')

    ax.set_xlabel('Time [s]')
    ax.set_yticks([r + bar_height for r in range(len(dict_2n_e1.keys()))], dict_2n_e1.keys())

    colors = {'2n_1e':'red', '2n_2e':'green', '2n_5e': 'blue', '5n_5e': 'black'}
    labels = list(colors.keys())
    handles = [plt.Rectangle((0,0),1,1, color=colors[label]) for label in labels]
    ax.legend(handles, labels)

    fg.savefig(GRAPH_TARGET_PATH + 'all_dbt_run_times_with_nodes.png')

# print_times(TPC_DI_NOTEBOOK_PATH_2n_1e)
# print_times(TPC_DI_NOTEBOOK_PATH_2n_2e)
# print_times(TPC_DI_NOTEBOOK_PATH_2n_5e)
# print_times(TPC_DI_NOTEBOOK_PATH_5n_5e)

# save_dicts_to_csv(
#     get_table_creation_times_dict(TPC_DI_NOTEBOOK_PATH_2n_1e),
#     get_table_creation_times_dict(TPC_DI_NOTEBOOK_PATH_2n_2e),
#     get_table_creation_times_dict(TPC_DI_NOTEBOOK_PATH_2n_5e),
#     get_table_creation_times_dict(TPC_DI_NOTEBOOK_PATH_5n_5e)
# )

plot_total_time(
    get_table_creation_times_dict(TPC_DI_NOTEBOOK_PATH_2n_1e),
    get_table_creation_times_dict(TPC_DI_NOTEBOOK_PATH_2n_2e),
    get_table_creation_times_dict(TPC_DI_NOTEBOOK_PATH_2n_5e)
)

plot_times(
    get_table_creation_times_dict(TPC_DI_NOTEBOOK_PATH_2n_1e),
    get_table_creation_times_dict(TPC_DI_NOTEBOOK_PATH_2n_2e),
    get_table_creation_times_dict(TPC_DI_NOTEBOOK_PATH_2n_5e)
)


plot_total_time_with_nodes(
    get_table_creation_times_dict(TPC_DI_NOTEBOOK_PATH_2n_1e),
    get_table_creation_times_dict(TPC_DI_NOTEBOOK_PATH_2n_2e),
    get_table_creation_times_dict(TPC_DI_NOTEBOOK_PATH_2n_5e),
    get_table_creation_times_dict(TPC_DI_NOTEBOOK_PATH_5n_5e)
)

plot_times_with_nodes(
    get_table_creation_times_dict(TPC_DI_NOTEBOOK_PATH_2n_1e),
    get_table_creation_times_dict(TPC_DI_NOTEBOOK_PATH_2n_2e),
    get_table_creation_times_dict(TPC_DI_NOTEBOOK_PATH_2n_5e),
    get_table_creation_times_dict(TPC_DI_NOTEBOOK_PATH_5n_5e)
)

print_markdown_table(
    get_table_creation_times_dict(TPC_DI_NOTEBOOK_PATH_2n_1e),
    get_table_creation_times_dict(TPC_DI_NOTEBOOK_PATH_2n_2e),
    get_table_creation_times_dict(TPC_DI_NOTEBOOK_PATH_2n_5e),
    get_table_creation_times_dict(TPC_DI_NOTEBOOK_PATH_5n_5e)
)