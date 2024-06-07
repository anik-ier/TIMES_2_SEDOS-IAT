import json
import os
import pandas as pd
import re
import datetime
# import unit metadata
from times_2_sedos import mapping_topology_w_unit, metadata_unit_SEDOS, metadata_unit_process


# Function to replace NaN with null in a JSON-like structure
def replace_nan_with_null(obj):
    if isinstance(obj, list):
        return [replace_nan_with_null(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: replace_nan_with_null(value) for key, value in obj.items()}
    elif isinstance(obj, float) and pd.isna(obj):
        return None
    else:
        return obj


# path of metadata JSON
metadata_path = "C:/Users/ac141435/Desktop/TIMES_2_SEDOS-IAT/metadata_template_AP9_IAT.json"
metadata_output = "C:/Users/ac141435/Desktop/TIMES_2_SEDOS-IAT/metadata/"
source_path = "C:/Users/ac141435/Desktop/TIMES_2_SEDOS-IAT/data_sources.xlsx"
# SEDOS_Modellstruktur
SEDOS_Modellstruktur_path = 'C:/Users/ac141435/Desktop/TIMES_2_SEDOS-IAT/SEDOS_Modellstruktur.xlsx'
# csv files folder and create list
csv_folder_path = "C:/Users/ac141435/Desktop/TIMES_2_SEDOS-IAT/csv/"
# path of mapping data from TIMES to SEDOS
mapping_excel_path = "C:/Users/ac141435/Desktop/TIMES_2_SEDOS-IAT/Mapping_TIMES_2_SEDOS_IAT.xlsx"


# process listed in SEDOS_Modellstruktur
def sedos_process_list(subsector_name):
    """
    @param subsector_name: name of the industry sub-sector in SEDOS style, str, e.g. 'ind_automobile'
    @return: list of processes from the specified sub-sector
    """
    sedos_process_ms = pd.read_excel(SEDOS_Modellstruktur_path, sheet_name='Process_Set')  # SEDOS_Modellstruktur
    sedos_ind_subsector = sedos_process_ms[sedos_process_ms['process'].str.contains(subsector_name)]
    sedos_ind_process_list = sedos_ind_subsector['process'].tolist()
    return sedos_ind_process_list


# list of all csv files and list of process without .csv extension
csv_files = []
for file in os.listdir(csv_folder_path):
    csv_files.append(file)
prepared_process_list = []
for process_csv in csv_files:
    prepared_process, _ = os.path.splitext(process_csv)
    prepared_process_list.append(prepared_process)
print(f"Number of process prepared as data given, {len(prepared_process_list)}")
# check, if all processes from SEDOS_Modellstruktur are prepared
SEDOS_process_check_list = sedos_process_list('ind_automobile')  # | ind_paper | ind_source | ind_copper | ind_aluminum | ind_cement')
print(f"Total process listed in SEDOS_Modellstruktur {len(SEDOS_process_check_list)}")
not_found_process = 0
for process_ms in SEDOS_process_check_list:
    if process_ms not in prepared_process_list:
        print(f"{process_ms}, is not found and processed")
        not_found_process += 1
    else:
        pass
print(f"Total number of process not found: {not_found_process}")

#  mapping parameters details into dict
parameter_sheet = pd.read_excel(mapping_excel_path, sheet_name=['SEDOS_parameters_1_e', 'SEDOS_parameters_1_w', 'SEDOS_parameters_GW'])


parameters_dict = {}
# to avoid duplication with 'fix, min, max'
parameters_to_ignore = ['capacity_p_abs_new', 'capacity_e_abs_new', 'capacity_w_abs_new', 'flow_share']
for dict_key, dict_value in parameter_sheet.items():
    dict_value = dict_value[['SEDOS', 'description', 'type', 'TIMES_Unit', 'isAbout']]  # use only necessary columns
    dict_value.set_index(['SEDOS'], inplace=True)  # set index with SEDOS parameters
    dict_value = dict_value.to_dict('index')  # convert into a dictionary by index
    # filter out keys(parameters) before updating
    filter_dict_value = {key: value for key, value in dict_value.items() if key not in parameters_to_ignore}
    parameters_dict.update(filter_dict_value)

# mapping of process and description
process_sheet = pd.read_excel(mapping_excel_path, 'SEDOS_process')
process_SEDOS = process_sheet[['SEDOS', 'description']]  # list of processes for SEDOS
process_dict = dict(zip(process_SEDOS['SEDOS'], process_SEDOS['description']))

# mapping of commodities and unit
commodity_sheet = pd.read_excel(mapping_excel_path, 'SEDOS_commodity')
commodity_sheet = commodity_sheet[['SEDOS', 'TIMES_Unit']]

# mapping of metadata unit, specific to each process
process_metadata_comm = mapping_topology_w_unit.groupby('Process')
# commodity unit as PJ/Mt or Kt/PJ
all_process_comm_unit = metadata_unit_SEDOS
# commodity unit as just PJ or MT or KT, approach is not used right now
# all_process_comm_unit = metadata_unit_process

"""
for process_name, process_group in process_metadata_comm:
    comm_unit = process_group.set_index('Commodity').to_dict()["metadata_unit"]
    process_comm_unit = {f"{process_name}": comm_unit}
    all_process_comm_unit.update(process_comm_unit)
print(all_process_comm_unit)
"""
# sources of processes data
source_proc = process_sheet[['SEDOS', 'source']]
source_proc_dict = dict(zip(process_sheet['SEDOS'], process_sheet['source']))
data_source = pd.read_excel(source_path, 'sources')

for process in csv_files:
    for process_name, process_description in process_dict.items():
        if process_name == process[:-4]:  # match the process name
            process_description = process_dict.get(process_name)
            if process_description is None:
                print(f"Process description is not found for: {process_name}")
                continue
            # print(process_name)
            with open(metadata_path, 'r') as file: # open and read JSON file
                metadata_template = json.load(file)
            # write details of a process from mapping and others
            metadata_template['name'] = metadata_template['name'] + process_name
            metadata_template['title'] = metadata_template['title'] + process_name
            metadata_template['id'] = metadata_template['id'] + process_name
            metadata_template['description'] = metadata_template['description'] + ' ' + process_description
            # print(metadata_template['description'])
            metadata_template['publicationDate'] = str(datetime.date.today())
            # print(metadata_template['publicationDate'])

            for contributor in metadata_template['contributors']:
                contributor['date'] = str(datetime.date.today())  # add date of contribution

            metadata_template['resources'][0]['name'] = metadata_template['resources'][0]['name'] + process_name
            metadata_template['resources'][0]['path'] = metadata_template['resources'][0]['path'] + process_name
            fields = metadata_template['resources'][0]['schema']['fields']  # fields of resources from metadata
            process_csv = os.path.join(csv_folder_path, process)  # csv files path
            process_df = pd.read_csv(process_csv, delimiter=';')

            for column in process_df.columns[3:]:  # iterate through columns of process from CSV
                item = dict()
                for key in parameters_dict:  # iterate through parameters dictionary
                    # if re.search(key, column):
                    if column.startswith(key): # match key from parameters dict with column name from process CSV
                        item["name"] = column
                        item["description"] = parameters_dict[key]["description"]
                        # print(parameters_dict[key]["description"])
                        # other than global emission (type: text) emission type is float
                        # if '_neg' in column or '_n2o' in column or '_ch4' in column:
                        if 'emi_co2_f_ind' in column:
                            item['type'] = 'text'
                        else:
                            item['type'] = parameters_dict[key]["type"]
                            # specific to each process, no matter material or energy commodity
                        for process_name_key, metadata_unit in all_process_comm_unit.items():
                            if process_name == process_name_key:  # match each process name with dict key(process name)
                                # iterate over value, whIAT holds conversion_factor (commodity) unit
                                for commodity_name, commodity_unit in metadata_unit.items():
                                    if "conversion_factor" in column:
                                        if str(commodity_name) in column:
                                            item['unit'] = commodity_unit
                                    elif "ef_" in column:
                                        # only consider emission commodity unit
                                        if (("emi_co2_p_ind" in commodity_name or "emi_co2_neg_proc_cc_ind" in commodity_name
                                          or "emi_ch4_p_ind" in commodity_name) or "emi_n2o_p_ind" in commodity_name
                                          or "emi_co2_neg_fuel_cc_ind" in commodity_name):
                                            item['unit'] = commodity_unit
                                        else:
                                            item['unit'] = parameters_dict[key]["TIMES_Unit"]
                                    else:
                                        item['unit'] = parameters_dict[key]["TIMES_Unit"]

                        item['isAbout'] = [json.loads(parameters_dict[key]["isAbout"])]
                        item['valueReference'] = []
                        fields.insert(-5, item)
                    # print(fields)
            metadata_template['resources'][0]['schema']['fields'] = fields  # insert fields value on metadata template

            source_meta = metadata_template["sources"]  # store source part from template
            # set to keep track of processed sources
            added_sources = set()
            for process_key, source in source_proc_dict.items():
                if process_key == process_name:
                    source_dict = eval(source)  # convert str into dictionary
                    unq_source_list = []

                    for key, val in source_dict.items():
                        # split sources whIAT are more than two source but for one parameter
                        for source_val in val.split(','):
                            source_val = source_val.strip()  # remove any whitespace
                            if source_val not in unq_source_list:
                                unq_source_list.append(source_val)
                        # print(unq_source_list)

                    for source_unq in unq_source_list:
                        if source_unq in added_sources:  # check if source is already processed
                            continue  # skip processing if source already processed
                        added_sources.add(source_unq)
                        # print(added_sources)

                        # source information df for each unique source
                        source_info = data_source[data_source['source_id'] == source_unq]

                        if source_info.empty:
                            print('Source ' + source_unq + 'is not found for ' + process_key)
                        else:  # construct metadata
                            for idx, row in source_info.iterrows():
                                info_source = dict()  # empty dict to store data
                                title = row['title']
                                description = row['description']
                                license_name = row['licenses_name']
                                info_source["title"] = row['title']
                                info_source["description"] = row['description']
                                info_source["path"] = row['path']
                                info_source["licenses"] = [
                                    {
                                        "name": row['licenses_name'],
                                        "title": row['licenses_title'],
                                        "path": row['licenses_path'],
                                        "instruction": row['licenses_instruction'],
                                        "attribution": row['licenses_attribution']
                                    }
                                ]
                                source_meta.append(info_source)  # append dict into template list
            # Replace NaN with null in the JSON-like structur
            metadata_template = replace_nan_with_null(metadata_template)
            # create json
            json_object = json.dumps(metadata_template, indent=4)
            # print(json_object)

            with open(metadata_output + '/' + process_name + '.json', "w") as out_file:
                out_file.write(json_object)
        # print(metadata_template['resources'][0]['schema']['fields'])


