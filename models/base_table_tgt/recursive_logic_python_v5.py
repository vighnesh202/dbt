
import os
import numpy as np
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import time

#BQ Connection
key_file_path = os.getcwd() + '/new_sa.json'
project_id_value = 'gcp-edwprdslsbtch-prd-90938'
#credentials = service_account.Credentials.from_service_account_file(key_file_path)
#connection = bigquery.Client(credentials=credentials, project=project_id_value)
credentials = service_account.Credentials.from_service_account_file(
    key_file_path
)

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
    print("current recursion: {}".format(recursive_count))
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


if __name__ == '__main__':
    # get the start time when the execution starts
    start_time = time.time()

    # get the base table into a dataframe
    base_sql = """
        SELECT 
            BK_POS_TRANSACTION_ID_INT,
            DV_SCRUBBED_SERIAL_NUM,
            BK_DETAIL_ID_INT,
            RNK
          FROM
          gcp-edwprddata-prd-33200.WORK_SALESDB.WI_POS_TRANS_LINE_SERIAL_NUM AS stg
    """  

    base_table_df = pd.read_gbq(base_sql,project_id=project_id_value,credentials=credentials)
    #base_table_df = pd.read_excel("data_validation/td_base_table.xlsx", keep_default_na=False )
    #replace(np.nan, 'N/A', regex=True)

    # execution of non recusrive logic
    #non_recursive_df = pd.read_gbq(non_recusrive_sql,project_id=project_id_value,credentials=credentials)
    non_recursive_df = base_table_df.loc[base_table_df['RNK'] == 1]
    
    # first append the non-recursive term result as it forms the base of the union all statement
    recursive_result_list.append(non_recursive_df)
    
    # start the recursion by calling the recursive function
    recursive_result_list = recusrion(non_recursive_df, base_table_df)


    concatenated_result = pd.concat(recursive_result_list)
    
    # execution of the sql logic part based on the recursive function result
    # concatenated_result['window_rank'] = concatenated_result.groupby(['BK_POS_TRANSACTION_ID_INT'])[
    #     'BK_DETAIL_ID_INT'].rank(method='min', ascending=False)
    # concatenated_result = concatenated_result.loc[concatenated_result['window_rank'] == 1, :]
    # concatenated_result.drop(columns=['window_rank'], inplace=True)
    
    #concatenated_result.to_excel("data_validation/concatenated_result.xlsx")
    
    concatenated_result.to_gbq(destination_table='gcp-edwprddata-prd-33200.WORK_SALESDB.WI_POS_TRANS_LINE_SERIAL_RECUSRIVE_RESULT_v6',
                                project_id=project_id_value,
                                credentials=credentials,
                                if_exists='replace')
    
    end_time = time.time()
    
    print("total timee taken: {}".format(end_time-start_time))
    print("Total recursion: {}".format(recursive_count))
    
    #436214725
    
    
    
    
# INSERT INTO  `gcp-edwprddata-prd-33200`.`WORK_SALESDB`.`WI_POS_TRANS_LINE_SERIAL`  (BK_POS_TRANSACTION_ID_INT,DV_SCRUBBED_SERIAL_NUM,BK_DETAIL_ID_INT ,RNK)   
#   SELECT
#       cast(xyz.bk_pos_transaction_id_int as int64),
#       xyz.dv_scrubbed_serial_num,
#       cast(xyz.bk_detail_id_int as int64),
#       xyz.rnk
#     FROM
#       `gcp-edwprddata-prd-33200`.`WORK_SALESDB`.`WI_POS_TRANS_LINE_SERIAL_RECUSRIVE_RESULT_v6` xyz
#     QUALIFY rank() OVER (PARTITION BY cast(xyz.bk_pos_transaction_id_int as int64) ORDER BY cast(xyz.bk_detail_id_int as int64) DESC) = 1;

# UPDATE `gcp-edwprddata-prd-33200`.`WORK_SALESDB`.`WI_POS_TRANS_LINE_SERIAL` AS wrvfc SET dml_type = stg.dml_type FROM (
#   SELECT
#       cast(wvf.bk_pos_transaction_id_int as int64) as bk_pos_transaction_id_int,
#       CASE
#         WHEN cast(mprv.bk_pos_transaction_id_int as int64) IS NULL THEN 'I'
#         ELSE 'U'
#       END AS dml_type
#     FROM
#         `gcp-edwprddata-prd-33200`.`WORK_SALESDB`.`WI_POS_TRANS_LINE_SERIAL` AS wvf
#       LEFT OUTER JOIN `gcp-edwprddata-prd-33200`.`BR_SALESDB`.`MT_POS_TRANS_LINE_SERIAL_NUM` AS mprv ON 
#       cast(wvf.bk_pos_transaction_id_int as int64) = cast(mprv.bk_pos_transaction_id_int as int64)
# ) AS stg WHERE cast(wrvfc.bk_pos_transaction_id_int as int64)  = cast(stg.bk_pos_transaction_id_int as int64); 
    