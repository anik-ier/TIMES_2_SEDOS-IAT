import numpy as np
import pandas as pd
import os
import json
import re
pd.set_option('display.max_columns', 50)

# paths of mapping data from TIMES to SEDOS
mapping_excel_path = "C:/Users/ac141435/Desktop/TIMES_2_SEDOS-IAT/Mapping_TIMES_2_SEDOS_IAT.xlsx"
source_path = "C:/Users/ac141435/Desktop/TIMES_2_SEDOS-IAT/data_sources.xlsx"  # data sources Excel file path
times_data_path = "C:/Users/ac141435/Desktop/TIMES_2_SEDOS-IAT/ind_auto_sedos_20240530_v01.xlsx"  # path of TIMES data
output_path = "C:/Users/ac141435/Desktop/TIMES_2_SEDOS-IAT/csv/"  # process CSV files output file path
demand_output_path = "C:/Users/ac141435/Desktop/TIMES_2_SEDOS-IAT/scalar_demand/"  # demand CSV file output file path

# create mapping dictionary for TIMES to SEDOS naming contents
mapping_data_xl = pd.ExcelFile(mapping_excel_path)
mapping_data = {}  # create dictionary to store data from each sheet
for sheet_name in mapping_data_xl.sheet_names:
    mapping_data_df = pd.read_excel(mapping_data_xl, sheet_name)
    mapping_dict = mapping_data_df.set_index('TIMES').to_dict()['SEDOS']  # create dictionary from Excel mapping
    mapping_data[sheet_name] = mapping_dict  # store data as dictionary from each mapping sheet

# mapping of process with and without 'GW' capacity, ('column 8' holds info on capacity, GW= 31.536)
mapping_topology = pd.read_excel(times_data_path, 'DE-TIDData')
mapping_topology = mapping_topology.drop(columns=mapping_topology.columns[[0, 5, 6, 7]])  # drop unnecessary columns
topology_new_headers = ['Parameters', 'Process', 'Commodity', 'Commodity_group', 'Capacity']  # add headers of topology
mapping_topology.columns = topology_new_headers
mapping_topology_commgrp = mapping_topology.copy()
mapping_CAPACT = mapping_topology[mapping_topology['Parameters'] == 'PRC_CAPACT']  # filter rows with PRC_CAPACT


# prepare unit for metadata unit and unit mapping
unit_mapping = pd.read_excel(mapping_data_xl, sheet_name="SEDOS_commodity")
unit_dict = unit_mapping.set_index('TIMES').to_dict()['TIMES_Unit']
# place unit for respective commodities
mapping_topology['unit'] = mapping_topology['Commodity'].map(lambda x: unit_dict.get(x, None))
# place unit for commodities if they are in commodity group
mapping_topology_commgrp['unit'] = mapping_topology_commgrp['Commodity_group'].map(lambda x: unit_dict.get(x, None))
# create unit as TOP-IN/TOP-OUT, e.g. PJ/PJ, PJ/Million ton, ignore emission
mapping_topology_w_unit = mapping_topology[((mapping_topology['Parameters'] == "TOP-IN") | ((mapping_topology['Parameters'] == "TOP-OUT")))]


# primary output unit from mapping topology
mapping_topology_pri_out = mapping_topology_commgrp[(mapping_topology_commgrp['Parameters'] == "PRC_ACTUNT")]
process_indv_pri_out = mapping_topology_pri_out.groupby('Process')
pri_out_dict = {}
for process_name_pri, process_group_pri in process_indv_pri_out:
    ind_pri_out = process_group_pri.set_index('Commodity_group')['unit'].to_dict()
    pri_out_dict[process_name_pri] = ind_pri_out


# create metadata unit for input and output
mapping_topology_w_unit = mapping_topology_w_unit.copy()
metadata_unit_df = pd.DataFrame()
process_indv = mapping_topology_w_unit.groupby('Process')
process_top_out_dict = {}
process_top_in_dict = {}
for process_name, process_group in process_indv:
    for index, row in process_group.iterrows():
        # metadata unit, with process emission
        if row['Parameters'] == "TOP-IN":
            top_out_row = process_group[(process_group["Process"] == row["Process"])
                                        & (process_group["Parameters"] == "TOP-OUT")
                                        & ~(process_group["Commodity"].str.contains('INDCO2N|INDSCO2N|INDCH4N|INDN2ON'))]
            # convert into dict
            top_out_dict = top_out_row.set_index('Commodity')['unit'].to_dict()
            process_top_out_dict[process_name] = top_out_dict
            if not top_out_row.empty:
                metadata_unit = f"{row['unit']}/{top_out_row['unit'].values[0]}"
                metadata_unit_df.at[index, "metadata_unit"] = str(metadata_unit)
            else:
                print(f"{row['Process']}, not part of SEDOS_Modellstruktur")
        # metadata unit for output, without process emission but combustion emission
        elif row['Parameters'] == 'TOP-OUT' and row['Commodity'] != 'INDCO2P' and row['Commodity'] != 'INDCH42P'\
                and row['Commodity'] != 'INDNO2P' and row['Commodity'] != 'INDSCO2P':
            top_in_row = process_group[(process_group["Process"] == row["Process"]) &
                                       (process_group["Parameters"] == "TOP-IN")]
            top_in_dict = top_in_row.set_index('Commodity')['unit'].to_dict()
            process_top_in_dict[process_name] = top_in_dict
            if not top_in_row.empty:
                metadata_unit = f"{row['unit']}/{top_in_row['unit'].values[0]}"
                metadata_unit_df.at[index, "metadata_unit"] = str(metadata_unit)


# metadata unit for input
metadata_unit_in_new = {}
for process_key_in, in_unit in process_top_in_dict.items():
    metadata_unit_in_new[process_key_in] = {}
    # Determine the out_unit from the corresponding entry in process_top_out_dict
    comm_units_out = pri_out_dict.get(process_key_in, {})
    for comm_in, comm_unit_in in in_unit.items():
        md_unit_in = f"{comm_unit_in}/{list(comm_units_out.values())[0]}"
        metadata_unit_in_new[process_key_in][comm_in] = md_unit_in

# metadata unit for output commodity including process emission
metadata_unit_out_new = {}
for process_key_out, out_unit in process_top_out_dict.items():
    metadata_unit_out_new[process_key_out] = {}
    comm_units_out = pri_out_dict.get(process_key_out, {})
    for comm_out, comm_unit_out in out_unit.items():
        for p_key, c_val in pri_out_dict.items():
            # check if commo_out is primary commodity
            if comm_out in c_val.keys() and process_key_out in p_key:
                md_unit_out = comm_unit_out
                # primary commodity should have just PJ or Mt in units not like PJ/Mt or Mt/Mt
                metadata_unit_out_new[process_key_out][comm_out] = md_unit_out
            # when commodity not in c_val.keys, it is not a primary commodity
            elif comm_out not in c_val.keys() and process_key_out in p_key:
                md_unit_out = f"{comm_unit_out}/{list(comm_units_out.values())[0]}"
                metadata_unit_out_new[process_key_out][comm_out] = md_unit_out

# creat dict of metadata unit for both input and output per process
metadata_unit_new = {}
# metadata_unit_in_new.keys() and metadata_unit_out_new.keys() get all the keys from both dictionaries
# set(...).union(...) creates a set that contains all unique keys from both dictionaries
# The ** operator is used to unpack the key-value pairs of each dictionary.
for key in set(metadata_unit_in_new.keys()).union(metadata_unit_out_new.keys()):
    if key in metadata_unit_in_new and key in metadata_unit_out_new:
        metadata_unit_new[key] = {**metadata_unit_in_new[key], **metadata_unit_out_new[key]}
    elif key in metadata_unit_in_new:
        metadata_unit_new[key] = metadata_unit_in_new[key]
    else:
        metadata_unit_new[key] = metadata_unit_out_new[key]

# final dict, whIAT is used to prepare input-output unit for metadata
metadata_unit_SEDOS = {}
for orig_key, orig_value in metadata_unit_new.items():
    process_key = mapping_data['SEDOS_process'].get(orig_key, orig_key)
    metadata_unit_SEDOS[process_key] = orig_value
    updated_value = {}
    for key, unit_value in orig_value.items():
        comm_key = mapping_data['SEDOS_commodity'].get(key, key)
        updated_value[comm_key] = unit_value
    metadata_unit_SEDOS[process_key] = updated_value
# print(metadata_unit_SEDOS)

# concatenate metadata_unit_df with original df (mapping_topology_w_unit)
mapping_topology_w_unit = pd.concat([mapping_topology_w_unit, metadata_unit_df], axis=1)
# processes with 'GW' Capacity PRC_CAPACT = 31.536
mapping_GW = mapping_CAPACT[mapping_CAPACT['Capacity'] == 31.536].set_index('Process')['Capacity'].to_dict()
# processes without 'GW' Capacity PRC_CAPACT = 1
mapping_1 = mapping_CAPACT[mapping_CAPACT['Capacity'] == 1].set_index('Process')['Capacity'].to_dict()
# processes without 'GW' Capacity PRC_CAPACT = 1 and PRC_ACTUNT belongs to commodity group (commodity)
mapping_CAP_ACTUNT = mapping_topology[(mapping_topology['Parameters'] == 'PRC_CAPACT') | (mapping_topology['Parameters'] == 'PRC_ACTUNT')]  # filter rows with PRC_CAPACT & PRC_ACTUNT
mapping_1_e_w = mapping_CAP_ACTUNT[mapping_CAP_ACTUNT['Capacity'] == 1]
mapping_1_e_w = mapping_1_e_w.copy()
mapping_1_e_w['unit'] = mapping_1_e_w['Commodity_group'].map(lambda x: unit_dict.get(x, None))
mapping_1_e = mapping_1_e_w[mapping_1_e_w['unit'] == 'PJ'].set_index('Process').to_dict()['Capacity']  # process with PJ
mapping_1_w = (mapping_1_e_w[(mapping_1_e_w['unit'] == 'Million units') | (mapping_1_e_w['unit'] == 'Million tonnes')].
               set_index('Process')).to_dict()['Capacity']

# clean TIMES-data dataframe
times_data_df = pd.read_excel(times_data_path, sheet_name='DE-TSData')  # read 'DE-TSData' as dataframe
# delete columns [0, 5, 8] > TS_DATA, empty and Int_extrapolation columns, column 19 holds last data year value
times_data_df = times_data_df.drop(columns=times_data_df.columns[[0, 5, 8]])
df_new_headers = ['parameters', 'process', 'commodity', 'commodity_group', 'time_slice', 'limit',
                  '2021', '2024', '2027', '2030', '2035', '2040', '2045', '2050', '2060', '2070']
times_data_df.columns = df_new_headers  # set columns headers

# create a dataframe for primary commodity
pri_out_comm = {}
for process, value in pri_out_dict.items():
    for pri_comm in value:
        pri_out_comm[process] = pri_comm

# convert dict into dataframe
pri_comm_df = pd.DataFrame(list(pri_out_comm.items()), columns=['process', 'commodity'])
pri_comm_df['parameters'] = 'FLO_EFF'  # add parameters column
years_col = ['2021', '2024', '2027', '2030', '2035', '2040', '2045', '2050', '2060', '2070']  # years columns
years_val = 1
for year in years_col:
    pri_comm_df[year] = years_val
# concatenate pri_comm_df (with primary output) with main times_data_df
times_data_df = pd.concat([pri_comm_df, times_data_df], ignore_index=True)


# remove any value on rows when NCAP_BND is 0
# get unique process from 'process' column
new_process = times_data_df['process'][times_data_df['process'].str.endswith(('01', '02', '03', '15', '20'))].unique()

for process in new_process:
    # columns to iterate, exclude mentioned columns
    col_to_check = times_data_df.columns.difference(['process', 'commodity', 'commodity_group', 'parameters', 'time_slice', 'limit'])
    for col in col_to_check:
        # creat boolean mask selecting rows where parameters columns has NCAP_BND, then selecting col for rows,
        # then values extract value
        if times_data_df.loc[(times_data_df['parameters'] == 'NCAP_BND') & (times_data_df['process'] == process), col].values[0] == 0:
            times_data_df.loc[(times_data_df['parameters'] != 'NCAP_BND') & (times_data_df['process'] == process), col] = np.nan


# replace ACTGRP with primary commodity
for key_proc, val_com in pri_out_comm.items():
    for index, row in times_data_df.iterrows():
        if key_proc == str(row['process']) and 'ACTGRP' in str(row['commodity_group']):
            times_data_df.at[index, 'commodity_group'] = val_com
        else:
            pass

# fill commodity columns with TOP-IN, when there is ACT_EFF with - in commodity and ACTGRP in commodity group
"""
top_in_df = mapping_topology[mapping_topology['Parameters'] == 'TOP-IN']
mapping_top_in = top_in_df.set_index('Process')['Commodity'].to_dict()
print(mapping_top_in)
for process, commodity in mapping_top_in.items():
    matched_row = (times_data_df['process'] == process) & (times_data_df['parameters'] == 'ACT_EFF')
    times_data_df.loc[matched_row, 'commodity'] = commodity
    #print(process, commodity)
"""

# only keep processes in times_data_df, whIAT are included in SEDOS
SEDOS_processes = set(mapping_data['SEDOS_process'].keys())
# filter times_data_df to keep only processes in SEDOS_process and 'COM_PROJ' (demand data)
times_data_df = times_data_df[times_data_df['process'].isin(SEDOS_processes) | times_data_df['parameters'].str.contains('COM_PROJ')]

# convert ACT_EFF into conversion_factor (FLO_EFF in ANSWER, e.g. ACT_EFF = 0.90 > FLO_EFF = 1/0.9 = 1.11)
""" 
ACT_EFF = 0.90 of gas boiler means, 1PJ of gas produces 0.90PJ heat. 
same ACT_EFF value can be expressed by conversion_factor (FLO_EFF)
conversion_factor_gas = 1/0.9 = 1.11 (for this boiler), whIAT means, 1.11PJ GAS would produce 1PJ heat
"""

act_eff_rows = times_data_df[times_data_df['parameters'] == "ACT_EFF"]
years_col = ['2021', '2024', '2027', '2030', '2035', '2040', '2045', '2050', '2060', '2070']
# convert ACT_EFF value into conversion_factor value
times_data_df.loc[act_eff_rows.index, years_col] = 1 / times_data_df.loc[act_eff_rows.index, years_col]
# rename ACT_EFF to conversion_factor
times_data_df.loc[act_eff_rows.index, "parameters"] = 'conversion_factor'

# Rename Parameters columns data with 'SEDOS' parameter names
# process with capacity in GW
for process, value in mapping_GW.items():
    matched_rows = times_data_df['process'] == process
    # rename parameter for Capacity in 'GW'
    times_data_df.loc[matched_rows, 'parameters'] = (times_data_df.loc[matched_rows, 'parameters'].replace
                                                     (mapping_data['SEDOS_parameters_GW']))
# process with capacity not in GW and with activity unit in PJ
for process, value in mapping_1_e.items():
    matched_rows = times_data_df['process'] == process
    # rename parameter for Capacity in '1'
    times_data_df.loc[matched_rows, 'parameters'] = (times_data_df.loc[matched_rows, 'parameters'].replace
                                                     (mapping_data['SEDOS_parameters_1_e']))

# process with capacity not in GW and with activity unit in 'Million units' or 'Million tonnes'
for process, value in mapping_1_w.items():
    matched_rows = times_data_df['process'] == process
    # rename parameter for Capacity in '1'
    times_data_df.loc[matched_rows, 'parameters'] = (times_data_df.loc[matched_rows, 'parameters'].replace
                                                     (mapping_data['SEDOS_parameters_1_w']))

# Rename Commodity and Commodity Group columns data with 'SEDOS' commodity names / alternative function
times_data_df['commodity'].replace(mapping_data['SEDOS_commodity'], inplace=True)

# rename commodity column from mapping_topology_w_unit
mapping_topology_w_unit['Commodity'].replace(mapping_data['SEDOS_commodity'], inplace=True)
mapping_topology_w_unit['Process'].replace(mapping_data['SEDOS_process'], inplace=True)

# create simple metadata unit for each commodity per process; e.g. PJ, Kt or Mt not like PJ/Million tonnes,
# PJ/Million tonnes, such units are prepared in metadata_unit_SEDOS
metadata_unit_process = {}
process_comm_unit = mapping_topology_w_unit.groupby('Process')
for process_name, process_data in process_comm_unit:
    comm_unit = process_data.set_index('Commodity')['unit'].to_dict()
    metadata_unit_process[process_name] = comm_unit

# rename commodity group with ACTGRP but has ACT_EFF with commodity group for a process
# for index, row in times_data_df.iterrows():
for key, value in mapping_data['SEDOS_commodity_group'].items():
    for index, row in times_data_df.iterrows():
        if row['process'].startswith(key.split('_')[0]) and 'ef' not in row['parameters']:
            #print(row['process'])
            times_data_df.at[index, 'commodity_group'] = value

# rename commodity group if given explicitly other than ACTGRP
times_data_df['process'].replace(mapping_data['SEDOS_process'], inplace=True)
times_data_df['commodity_group'].replace(mapping_data['SEDOS_commodity_group'], inplace=True)
# rename commodity_group name, if there is commodity (fuel) in commodity_group; e.g. fossil fuel, FLO_EMISS
times_data_df['commodity_group'].replace(mapping_data['SEDOS_commodity'], inplace=True)
times_data_df['limit'].replace(mapping_data['SEDOS_limit'], inplace=True)

# convert, if any datatype other than string into string
times_data_df['process'] = times_data_df['process'].astype(str)
times_data_df['commodity'] = times_data_df['commodity'].astype(str)
times_data_df['commodity_group'] = times_data_df['commodity_group'].astype(str)

# create SEDOS parameter (specific to each commodity, commodity group or constraints)
times_data_df['SEDOS_Parameters'] = ""

# split the commodities of a commodity groups and copy same value of group into each commodity
new_rows = []
for index, row in times_data_df.iterrows():
    # look for commodity group that has []
    if row['parameters'] == 'conversion_factor' and '[' in row['commodity_group'] and ']' in row['commodity_group']:
        com_grp = row['commodity_group'].strip('[]').split(',')  # split commodities of commodity group
        # print(com_grp)
        for item in com_grp:
            new_row = row.copy()  # copy value of commodity group
            new_row['commodity'] = item
            new_rows.append(new_row)  # append matched row in new_rows list
new_rows_df = pd.DataFrame(new_rows)  # create a dataframe of new rows to concat with main dataframe
times_data_df = pd.concat([times_data_df, new_rows_df], ignore_index=True)  # add new rows in main dataframe

# delete duplicated row with conversion factor, specific columns to check
times_data_df.drop_duplicates(['parameters', 'process', 'commodity', 'commodity_group'], keep='last', inplace=True)

# multiply 'flow_share' and 'availability_constant' values with 100 to convert those in % value
percent_param_mask = (times_data_df['parameters'].str.contains('flow_share') | times_data_df['parameters'].
                 str.contains('availability_constant'))
times_data_df.loc[percent_param_mask, years_col] *= 100


"""
- act_bnd + limit
- conversion_factor + commodity
- flow_emis + commodity + comm_grp
- flow_share + limit + commodity
"""
#print(times_data_df[times_data_df['process'] == 'ind_automobile_boiler_hot_water_bio_1'])

# prepare and create OEP data structure for SEDOS
for index, row in times_data_df.iterrows():
    if row['parameters'] == 'conversion_factor':
        # match index with condition and create SEDOS specific parameters for SEDOS_Parameters column
        times_data_df.at[index, 'SEDOS_Parameters'] = row['parameters'] + '_' + row['commodity']
    elif row['parameters'] == 'ef':
        if 'emi_co2_neg_fuel_cc_ind' in row['commodity']:  # take in account negative emission in CCS that has 'neg' in name
            #  create SEDOS parameter for negative combustion emission parameter
            times_data_df.at[index, 'SEDOS_Parameters'] = (row['parameters'] + '_' + row['commodity_group'] + '_' +
                                                           row['commodity'])  # row['commodity'][4:7])
        # take in account negative process emission in CC or process emission
        elif 'emi_co2_neg_proc_cc_ind' in row['commodity'] or 'emi_co2_p_ind' in row['commodity']\
                or 'emi_ch4_p_ind' in row['commodity'] or 'emi_n2o_p_ind' in row['commodity']:
            # create SEDOS parameter for negative process emission parameter> ef<commodity>emi_comm
            times_data_df.at[index, 'SEDOS_Parameters'] = ((row['parameters'] + '_' + row['commodity_group']) +
                                                           '_' + row['commodity'])
        elif 'emi_ch4_f_ind' in row['commodity'] or 'emi_n2o_f_ind' in row['commodity']:
            # create SEDOS parameter for CH4 and N2O emission parameter (this should be different from regular emission)
            times_data_df.at[index, 'SEDOS_Parameters'] = (row['parameters'] + '_' + row['commodity_group'] + '_' +
                                                           row['commodity'])
        # emission commodity CO2 from combustion
        elif 'emi_co2_f_ind' in row['commodity']:# and row['parameters'] == 'NCAP_BND':
            # create SEDOS parameter for emission
            times_data_df.at[index, 'SEDOS_Parameters'] = (row['parameters'] + '_' + row['commodity_group'] + '_'
                                                           + row['commodity'])
            milestone_years = ['2021', '2024', '2027', '2030', '2035', '2040', '2045', '2050', '2060', '2070']
            # create global_emission value and replace numerical value,
            # 'global_emi' is new column to hold global_emission text, create new value, e.g. global_emi.CH4_commodity
            if row['parameters'] == 'ef':
                if times_data_df.loc[index, milestone_years].notnull().any():
                    # Create global_emission value and replace numerical value
                    global_emi_value = 'global_emission_factors.' + row['commodity_group'] + '_' + row['commodity'][4:7]
                    times_data_df.loc[index, 'global_emi'] = global_emi_value

                    # Replace numerical values in milestone years with the global_emi value where they exist
                    for year in milestone_years:
                        if pd.notnull(times_data_df.at[index, year]) and isinstance(times_data_df.at[index, year],
                                                                                    (int, float)):
                            times_data_df.at[index, year] = global_emi_value
                else:
                    # If no milestone years have values, do not assign 'global_emi'
                    times_data_df.loc[index, 'global_emi'] = None
            """
            times_data_df.loc[times_data_df['parameters'] == 'ef', 'global_emi'] = (
                    ('global_emission_factors.' + times_data_df['commodity_group']) + '_' +
                    times_data_df['commodity'].str.slice(start=4, stop=7))
            # assign global emission value only for non 'neg' emission
            times_data_df.loc[index, milestone_years] = str(times_data_df.loc[index, 'global_emi'])
            """
    elif 'flow_share' in row['parameters']:
        times_data_df.at[index, 'SEDOS_Parameters'] = row['parameters'] + '_' + row['limit'] + '_' + row['commodity']
    elif ('max' in row['limit'] or 'min' in row['limit'] or 'fix' in row['limit']) and ('flow_share' not in
          row['parameters'] and 'capacity_e_abs_new' not in row['parameters'] and 'capacity_w_abs_new' not in
          row['parameters'] and 'capacity_p_abs_new' not in row['parameters'] and
          'availability_constant' not in row['parameters']):
        times_data_df.at[index, 'SEDOS_Parameters'] = row['parameters'] + '_' + row['limit']
    elif ('capacity_e_abs_new' in row['parameters'] or 'capacity_w_abs_new' in row['parameters'] or
          'capacity_p_abs_new' in row['parameters']):
        times_data_df.at[index, 'SEDOS_Parameters'] = row['parameters'] + '_' + 'max'

    elif 'exo' in row['commodity']:  # exo_demand data process
        times_data_df.loc[index, 'SEDOS_Parameters'] = row['commodity']
        times_data_df.loc[index, 'process'] = 'ind_automobile_scalars'
    else:
        times_data_df.at[index, 'SEDOS_Parameters'] = row['parameters']

# delete rows with 'conversion_factor_-', whIAT is from act_eff in commodity group issue
times_data_df = times_data_df[times_data_df['SEDOS_Parameters'] != 'conversion_factor_-']

print(times_data_df[times_data_df['process'] == 'ind_automobile_boiler_hot_water_bio_1'])

# drop columns that aren't needed anymore for SEDOS_data and create SEDOS_data dataframe
SEDOS_data = times_data_df.drop(
    columns=['parameters', 'commodity', 'commodity_group', 'time_slice', 'limit', 'global_emi'])
SEDOS_data = SEDOS_data.set_index(['process'])  # set row index by process

# create each process data frame into OEP structure
unq_process = SEDOS_data.index.unique()  # index of each process to create dataframe for each process
process_df = []
process_e_list = []  # empty list to store all process_df
for process in unq_process:
    process_df = SEDOS_data.loc[SEDOS_data.index == process]  # extract all data related with specific process
    process_df = process_df.T  # transpose dataframe to match sedos_oep data structure
    process_df.columns = process_df.loc['SEDOS_Parameters']  # rename column header with 'SEDOS_Parameters' row
    process_df = process_df.iloc[:-1]  # drop 'SEDOS_Parameters' row
    process_df.reset_index(drop=False, inplace=True)  # reset index that was somehow years after transposing
    process_df.rename(columns={'index': 'year'}, inplace=True)  # rename 'index' column as 'year' column
    # insert id, region and process name as 'type' columns at the beginning of dataframe
    process_df.insert(0, 'id', [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])  # need to be adjusted based on years*
    process_df.insert(1, 'region', 'DE')
    process_df.insert(3, 'type', process)
    # insert version, method, source and comment columns at the end of dataframe
    columns_oth = ['bandwidth_type', 'version', 'method', 'source', 'comment']
    sources_col = pd.read_excel(mapping_excel_path, 'SEDOS_process')
    for idx, row in sources_col.iterrows():
        if row['SEDOS'] == process:  # match process name from source df
            process_df['bandwidth_type'] = "{}"
            process_df['version'] = 'v1'
            process_df['method'] = row['method']
            process_df['source'] = row['source']
            process_df['comment'] = row['comment']
    # check if there are any NaN value in dataframe
    # check nan value in each column
    nan_column = process_df.isna().any()
    column_with_nan = nan_column[nan_column].index.tolist()
    # print('columns with Nan values, process: ' f'{process}, ', 'columns:', column_with_nan)
    # check, if any process from SEDOS_Modellstruktur is missing

    # save each process as CSV into folder
    if 'scalars' in process:
        process_df.to_csv(demand_output_path + process + '.csv', index=False, sep=';')
    else:
        process_df.to_csv(output_path + process + '.csv', index=False, sep=';')  # to use ; as delimiter> sep=';'

# print(process_df.columns)
# print(process_df)


