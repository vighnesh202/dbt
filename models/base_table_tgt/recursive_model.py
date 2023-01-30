import numpy as np
import pandas as pd

# default values
dv_scrubbed_serial_num_threshold = 2000
dv_scrubbed_serial_num_string_limit = 2000

# a list to hold all the individual recursion results
recursive_result_list = []

# def DV_SCRUBBED_SERIAL_NUM_case_statement(data):
#     DV_SCRUBBED_SERIAL_NUM_value = data['DV_SCRUBBED_SERIAL_NUM_x'].strip()
#     if data['DV_SCRUBBED_SERIAL_NUM_y'].strip() != '':
#         DV_SCRUBBED_SERIAL_NUM_value = DV_SCRUBBED_SERIAL_NUM_value + "," + data['DV_SCRUBBED_SERIAL_NUM_y'].strip()
#     print(DV_SCRUBBED_SERIAL_NUM_value)
#     return DV_SCRUBBED_SERIAL_NUM_value[0:dv_scrubbed_serial_num_string_limit]

recursive_count  = 0

def recusrion(recursion_df, base_table_df):
    # Execute recursive logic with Ri as an input to return the result set Ri+1 as the output, until
    # an empty result set is returned
    global recursive_count
    global recursive_result_list
    recursive_count += 1
    # xyz recursive logic
    # An empty dataframe to hold the recursive result set Ri
    recusrive_result = pd.DataFrame()
    
    # the first part is to do the from statement
    # Inner join between the recursive result and base table
    p1 = recursion_df.merge(base_table_df, left_on='BK_POS_TRANSACTION_ID_INT',
                                right_on='BK_POS_TRANSACTION_ID_INT', how='inner')

    # second part is to execute where clause
    p2 = p1.loc[(np.array(p1['RNK_y']) == (np.array(p1['RNK_x'])+1))
                          & ((p1['DV_SCRUBBED_SERIAL_NUM_x'].str.len() + p1['DV_SCRUBBED_SERIAL_NUM_y'].str.len()) < dv_scrubbed_serial_num_threshold)]

    # select clause
    recusrive_result['BK_POS_TRANSACTION_ID_INT'] = p2['BK_POS_TRANSACTION_ID_INT']
    
    
    recusrive_result['DV_SCRUBBED_SERIAL_NUM'] = p2['DV_SCRUBBED_SERIAL_NUM_x'].str.strip() + \
        ',' + p2['DV_SCRUBBED_SERIAL_NUM_y'].str.strip()
    
    recusrive_result['DV_SCRUBBED_SERIAL_NUM'] = recusrive_result['DV_SCRUBBED_SERIAL_NUM'].apply(lambda x: x[0:dv_scrubbed_serial_num_string_limit].strip(','))
    
    #recusrive_result['DV_SCRUBBED_SERIAL_NUM'] = recusrive_result['DV_SCRUBBED_SERIAL_NUM'].str[0:dv_scrubbed_serial_num_string_limit]

    recusrive_result['BK_DETAIL_ID_INT'] = p2['BK_DETAIL_ID_INT_y']
    recusrive_result['RNK'] = p2['RNK_y']
    
    # check for possibility for next iteration
    if not recusrive_result.empty:
        # append the result set to a global list variable
        recursive_result_list.append(recusrive_result)
        # Recursively call the recursion function, until an empty set is returned
        return recusrion(recusrive_result, base_table_df)
    
    return recursive_result_list

def model(dbt, session):
    dbt.config(
        materialized = 'table',
        submission_method = 'serverless'
    )
    global recursive_result_list
    global recursive_count
    
    # get the base table into a dataframe
    base_table_df = dbt.source('dbt_workdb', 'base_table').toPandas()
    
    # execution of non recusrive logic
    #non_recursive_df = pd.read_gbq(non_recusrive_sql,project_id=project_id_value,credentials=credentials)
    non_recursive_df = base_table_df.loc[base_table_df['RNK'] == 1]
    
    # first append the non-recursive term result as it forms the base of the union all statement
    recursive_result_list.append(non_recursive_df)
    
    # start the recursion by calling the recursive function
    recursive_result_list = recusrion(non_recursive_df, base_table_df)


    concatenated_result = pd.concat(recursive_result_list)
    
    return concatenated_result