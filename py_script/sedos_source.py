import pandas as pd
source_xl = "C:/Users/ac141435/Desktop/TIMES_2_SEDOS-IAT/process_source_IAT.xlsx"
source_mapping_output_path = "C:/Users/ac141435/Desktop/TIMES_2_SEDOS-IAT/"

source_raw = pd.read_excel(source_xl)
print(source_raw)
sedos_method = {}
sedos_source = {}
sedos_comment = {}

source_raw_proc = source_raw.groupby('process')

for process, process_df in source_raw_proc:
    method_prep = {}
    source_prep = {}
    comment_prep = {}
    for index, row in process_df.iterrows():
        # SEDOS method
        method_prep[row['columns']] = row['method']
        sedos_method[process] = method_prep
        # SEDOS source
        source_prep[row['columns']] = row['source']
        sedos_source[process] = source_prep
        # SEDOS comment
        comment_prep[row['columns']] = row['comment']
        sedos_comment[process] = comment_prep


print(sedos_source)
print(sedos_comment)
sedos_process_source_mapping = {}
for key in sedos_source.keys():
    sedos_process_source_mapping[key] = {}  # new dict under common key
    # merge value form all three dict
    sedos_process_source_mapping[key].update({'method': sedos_method[key], 'source': sedos_source[key], 'comment': sedos_comment[key]})

final_sedos_source_df = pd.DataFrame.from_dict(sedos_process_source_mapping, orient='index')
final_sedos_source_df.index.name = 'SEDOS'

final_sedos_source_df.to_excel(source_mapping_output_path + 'process_source_mapping.xlsx', sheet_name='SEDOS_process')
