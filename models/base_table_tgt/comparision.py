from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import os
import warnings
import pyodbc

warnings.simplefilter("ignore", UserWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)

# TERADATA CONNECTION
'''#user = input('Uid:')
#password = maskpass.askpass(prompt="Password:",mask="#")'''
link = 'Driver={Teradata Database ODBC Driver 16.20};DBCName=TDPROD;Uid=hsimhadr; Pwd=Sh!ma0920; Authentication=LDAP; Database=hsimhadr'
conn = pyodbc.connect(link)
cus = conn.cursor()

#BQ Connection
key_file_path = os.getcwd() + '/new_sa.json'
project_id = 'gcp-edwprdslsbtch-prd-90938'
credentials = service_account.Credentials.from_service_account_file(key_file_path)
connection = bigquery.Client(credentials=credentials, project=project_id)
key_file_path = os.getcwd() + '/new_sa.json'
project_id = 'gcp-edwprdslsbtch-prd-90938'
credentials = service_account.Credentials.from_service_account_file(key_file_path)

# Reading the mapping file
file_list = os.listdir("input_folder/")
for file in file_list:
    file_name = file
    input_file = os.getcwd() + "\\input_folder\\" + file
    output_file = open(os.getcwd() + "\\output_folder\\" + file, "a")
    with open(input_file, 'r') as file:
        data = file.readlines()
    for x in data:
        if x.startswith("#"):
            print("#---------------------------------------------------------------------------------------------------------------------#",
                  file=output_file)
        else:
            print(x)
            try:
                td_part = x.split("|")[0].strip()
                bq_part = x.split("|")[1].strip()
            except:
                print("Element is: {}".format(x), file=output_file)
                print("The Input element seems to be have missing elements \n", file=output_file)
                print(
                    "#---------------------------------------------------------------------------------------------------------------------#",
                    file=output_file)
                continue
            
            td_database_name = td_part.split('.')[0].upper().strip()
            td_table_name = td_part.split('.')[1].upper().strip("\n").strip()
            
            bq_dataset_name = bq_part.split('.')[0].upper().strip()
            bq_table_name = bq_part.split('.')[1].upper().strip("\n").strip()
            
            print(td_database_name+"."+td_table_name)
            print(bq_dataset_name+"."+bq_table_name+"\n")
            print("************")
            if not td_database_name or not td_table_name or not bq_dataset_name or not bq_table_name:
                print("Element is: {}".format(x), file=output_file)
                print("The Input element seems to be have missing elements \n", file=output_file)
                print(
                    "#---------------------------------------------------------------------------------------------------------------------#",
                    file=output_file)
                continue

            print("TD SCHEMA: ", td_database_name+"."+td_table_name, file=output_file)
            print("BQ SCHEMA: ", bq_dataset_name+"."+bq_table_name+"\n", file=output_file)

            Teradata_query = "SELECT ColumnName FROM dbc.columns WHERE tablename ='{}' and databasename ='{}' order by columnid".format(
                td_table_name, td_database_name)
            res = cus.execute(Teradata_query)
            cols = []
            Teradata_cols = []
            rows = res.fetchall()
            c=0
            for row in rows:
                c+=1
                
                cols.extend(row)
            print(c)
            for i in cols:
                j = i.replace(' ', '')
                Teradata_cols.append(j)

            # BIGQUERY CONNECTION
            connection = bigquery.Client(credentials=credentials, project=project_id)
            BQ_cols = []
            sql_text = """
                        SELECT column_name FROM gcp-edwprddata-prd-33200.{}.INFORMATION_SCHEMA.COLUMNS
                        where table_name = '{}'
                    """.format(bq_dataset_name, bq_table_name)
            query_results = (connection.query(sql_text).result())
            for row in query_results:
                BQ_cols.extend(row)

            # Check for the table existence in Teradata
            if not Teradata_cols:
                print("TABLE: {} DOESNOT EXIST IN TD".format(td_database_name+"."+td_table_name), 
                      file=output_file)
            if not BQ_cols:
                print("TABLE: {} DOESNOT EXIST IN BQ \n".format(bq_dataset_name+"."+bq_table_name),
                      file=output_file)

            # Both teredata and BQ Tables exists
            if Teradata_cols and BQ_cols:
                # create td dataframe
                td_dict = {'columns': Teradata_cols}
                td_df = pd.DataFrame(td_dict)
                td_df['index_col'] = td_df.index
        
                # create bq dataframe
                bq_dict = {'columns': BQ_cols}
                bq_df = pd.DataFrame(bq_dict)
                bq_df['index_col'] = bq_df.index
        
                # compare the column count of td and bq
                if len(td_df) != len(bq_df):
                    print("COULMN COUNT of TD: {} DOESNOT MATCH THE COLUMN COUNT OF BQ: {} \n".format(
                        len(td_df), len(bq_df)), file=output_file)
                else:
                    print("Column Count of TD and BQ Matches: {} \n".format(len(td_df)),
                          file=output_file)
        
                # Compare TD and BQ dataframe to find any missing column 
                custom_map = {"left_only": "Only in TD", "right_only": "Only in BQ", "both": "both"}
                compare = pd.merge(td_df, bq_df, 
                                   left_on = td_df["columns"].str.lower(),
                                   right_on = bq_df["columns"].str.lower(),
                                   how="outer", indicator=True)
                compare['_merge'] = compare['_merge'].map(custom_map)
                compare = compare.rename({'key_0': 'columns', 
                                              '_merge':'missing_columns'}, axis=1)
                
                # comparision to check if bq and td have missing columns on both sides
                compare_missing_columns = compare[compare['missing_columns'] != 'both']
                if compare_missing_columns.empty:
                    print("TD and BQ have similar column information on both sides \n",
                          file=output_file)
    
                    # comparing whether both td and bq column order are same
                    compare_column_order = compare[compare['index_col_x'] != compare['index_col_y']]
                    if compare_column_order.empty:
                        print("TD and BQ have same column order \n",
                              file=output_file)
                    else:
                        print("TD AND BQ COLUMN ORDER ARE DIFFERENT \n",
                              file=output_file)
                        print(compare_column_order, file=output_file)
                else:
                    print("TD AND BQ COLUMNS INFORMATION ARE NOT SIMILAR ON BOTH SIDES \n",
                          file=output_file)
                    compare_missing_columns = compare_missing_columns[['columns',
                                                                       'missing_columns']]
                    print(compare_missing_columns,  file=output_file)
    
            print(
                "#---------------------------------------------------------------------------------------------------------------------#",
                file=output_file)
    output_file.close()        
conn.close()
connection.close()