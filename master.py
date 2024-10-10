import os
from sys import exit
import pathlib

import json
import pandas as pd


BASE_DIR = str(pathlib.Path().resolve())
TARGET_DIR = ''
WORKING_DIR = f'{BASE_DIR}/{TARGET_DIR}'


class App:
    def __init__(self):
        # 
        self.dir_list = os.listdir(WORKING_DIR)
        self.mapping_dict = {}
        self.mapping_filename = ''
        self.main_df = ''
        self.enc = 'utf-8'
        self.db_type = 0  # 1: Oracle, 2: MSSQL
        self.schtbl_json_max_cnt = 99
        # CBLOB
        self.TakeOnlyCBlobTables = 2  # 0: init select, 1: Yes, 2: No
        self.IsCBlobTableIgnore = 1   # 0: init select, 1: Yes, 2: No
        self.IsCBlobColumnIgnore = 2  # 0: init select, 1: Yes, 2: No
        # CHEATS
        self.custom_schema_s_name = ''
        self.ignore_table_s_list = []
        self.ignore_code_s_list = []
        self.take_only_table_s_list = []
    
    def selection_block(self):
        # select db_type
        if not self.db_type:
            self.db_type = int(input(
                '\nChoose DB type:\n1: Oracle\n2: MSSQL\nYour choice: '
            ))

        # check db type
        if self.db_type not in (1,2):
            print('=DB CHOOSE ERROR=')
            print(f"db_type: {self.db_type}")
            self.pause()
            exit()

        # select cblob behavior        
        if self.db_type == 1 and \
                not self.TakeOnlyCBlobTables and \
                not self.IsCBlobTableIgnore and \
                not self.IsCBlobColumnIgnore:
            self.TakeOnlyCBlobTables = int(input(
                '\nTake only tables with CLOB/BLOB attributes for Oracle?:'
                '\n1: Yes\n2: No\n'
            ))
            if self.TakeOnlyCBlobTables == 2:
                self.IsCBlobTableIgnore = int(input(
                    '\nIgnore tables with CLOB/BLOB attributes for Oracle?:'
                    '\n1: Yes\n2: No\n'
                ))
            if self.IsCBlobTableIgnore == 2:
                self.IsCBlobColumnIgnore = int(input(
                    '\nIgnore CLOB/BLOB attributes for Oracle?:'
                    '\n1: Yes\n2: No\n'
                ))
        

    def pause(self):
        return input("\nPress the <ENTER> key to exit...")

    def parse_directory(self):
        
        print('=PARSING=')

        filtered_files = [
            filename for filename in self.dir_list
                if filename.endswith('.xlsx')
            ]

        self.mapping_dict = {
            number + 1: filename for number, filename
                in enumerate(filtered_files)
            }

    def make_df(self):
        
        print('=MAKING DF=')
        
        if not self.mapping_dict:
            print('\n==================================================')
            print(f'{"!В папке отсутствует mapping файл формата xlsx!":^50}')
            print('==================================================')
            self.pause()
            exit()
        elif len(self.mapping_dict) == 1:
            self.mapping_filename = next(iter(self.mapping_dict.values()))
        elif len(self.mapping_dict) > 1:
            for key in self.mapping_dict.keys():
                print(key,': ', self.mapping_dict[key])
                
            mapping_key = input('\nWrite mapping number: ')

            self.mapping_filename = self.mapping_dict[int(mapping_key)]

        try:
            print("Reading mapping file...")
            main_df = pd.read_excel(f'{WORKING_DIR}/{self.mapping_filename}',
                                    sheet_name='Mapping',
                                    usecols="D,"  # Source Schema
                                            "E,"  # Source Table
                                            "G,"  # Source Code
                                            "I,"  # Source Data Type
                                            "J,"  # Source Length
                                            "T,"  # Target Schema
                                            "U,"  # Tagret Table
                                            "V")  # Target Code
        except Exception as e:
            print("Error while reading file: ", e)
            self.pause()

        main_df = main_df.drop(0,axis=0)

        main_df.columns = ['SchemaS', 'TableS', 'CodeS',
                           'Data Type', 'Length',
                           'SchemaT', 'TableT', 'CodeT']
        
        main_df = main_df.fillna('')

        #
        main_df['SchemaS'] = main_df['SchemaS'].apply(lambda x: str(x).strip())
        main_df['TableS'] = main_df['TableS'].apply(lambda x: str(x).strip())
        main_df['CodeS'] = main_df['CodeS'].apply(lambda x: str(x).strip())
        main_df['Data Type'] = main_df['Data Type']\
                                        .apply(lambda x: str(x).strip())
        main_df['Length'] = main_df['Length'].apply(lambda x: str(x).strip())
        main_df['SchemaT'] = main_df['SchemaT'].apply(lambda x: str(x).strip())
        main_df['TableT'] = main_df['TableT'].apply(lambda x: str(x).strip())
        main_df['CodeT'] = main_df['CodeT'].apply(lambda x: str(x).strip())

        #
        main_df = main_df[main_df['CodeT']!='hdp_processed_dttm']
        
        # CBLOB
        if self.TakeOnlyCBlobTables == 1:
            cblob_table_df = main_df[
                    main_df['Data Type'].isin(['CLOB', 'BLOB'])
                ]
            cblob_table_list = cblob_table_df['TableS'].unique().tolist()
            if cblob_table_list:
                main_df = main_df[main_df['TableS']\
                                    .isin(cblob_table_list)]
        elif self.IsCBlobTableIgnore == 1:
            cblob_table_df = main_df[
                    main_df['Data Type'].isin(['CLOB', 'BLOB'])
                ]
            ignore_cblob_table_list = cblob_table_df['TableS'].unique().tolist()
            if ignore_cblob_table_list:
                main_df = main_df[~main_df['TableS']\
                                    .isin(ignore_cblob_table_list)]
        elif self.IsCBlobColumnIgnore == 1:
            main_df = main_df[~main_df['Data Type'].isin(['CLOB', 'BLOB'])]
        
        # CHEAT
        if self.custom_schema_s_name:
            main_df['SchemaS'] = self.custom_schema_s_name
        
        #
        main_df['schemaS.tableS'] = main_df['SchemaS'] +'.'+ main_df['TableS']

        # CHEAT
        if self.take_only_table_s_list:
            main_df = main_df[main_df['TableS'].isin(self.take_only_table_s_list)]

        if self.ignore_table_s_list:
            main_df = main_df[~main_df['TableS'].isin(self.ignore_table_s_list)]

        if self.ignore_code_s_list:
            main_df = main_df[~main_df['CodeS'].isin(self.ignore_code_s_list)]

        #
        main_df = main_df.sort_values(['TableS'])

        main_df.index = range(1, len(main_df) + 1)
        
        self.main_df = main_df
        
        print(self.main_df.head())
        
    def generate_json(self):
        
        print('=GENERATING JSON=')

        #
        self.selection_block()

        schema_t = self.main_df.iloc[0]['SchemaT']
        print(f'Target Schema: {schema_t}')
        test_flow_entity_lst = []
        
        # get schemaS.tables from mapping
        schemaS_tableS_lst = self.main_df['schemaS.tableS'].unique()
        #
        schtbl_len = len(schemaS_tableS_lst)
        print(f"Number of source schema.tables: {schtbl_len}")

        schtbl_cnt_trigger = 0
        schtbl_num = 1

        if self.db_type == 1:  # Oracle
            # generate oracle flows
            print('=MAKING ORACLE FLOWS=')
            
            for schema_table in schemaS_tableS_lst:

                current_df = self.main_df[
                    self.main_df['schemaS.tableS'] == schema_table
                    ]

                schema_s = current_df.iloc[0]['SchemaS']
                source_table = current_df.iloc[0]['TableS']
                target_table = current_df.iloc[0]['TableT']
                
                query_full = ''
                query_prefix = 'select '
                query_suffix = ' from $schema.$table'
                query_cast_list = []

                for _, row in current_df.iterrows():
                    target_column_name = row['CodeT']
                    source_column_name = row['CodeS']
                    source_column_type = row['Data Type']
                    source_column_length = ''

                    if row['Length'] and\
                        row['Data Type'].lower() not in ('smallint',
                                                         'date',
                                                         'int',
                                                         'integer'):
                        source_column_length = f"({row['Length']})"
                    elif row['Data Type'].lower() == 'varchar2':
                        source_column_length = '(255)'
                    else:
                        source_column_length = ''

                    query_cast_list.append(
                        f"cast('{source_column_name}' as "
                        f"{source_column_type}{source_column_length}) as "
                        f"'{target_column_name}'"
                        )

                query_full = ', '.join(query_cast_list)

                query_full = query_prefix + query_full + query_suffix

                flow_template = {
                    "loadType": "Scd1Replace",
                    "source": {
                        "schema": schema_s,
                        "table": source_table,
                        "query": query_full,
                        "jdbcDialect": "OracleDialect"
                    },
                    "target": {
                        "table": target_table
                    }
                }
                
                if schtbl_cnt_trigger < self.schtbl_json_max_cnt:

                    schtbl_cnt_trigger += 1

                    test_flow_entity_lst.append(flow_template)
                
                else:
                    
                    schtbl_cnt_trigger = 0

                    test_flow_entity_lst.append(flow_template)

                    self.print_results(schema_t,
                                       test_flow_entity_lst,
                                       schtbl_num,
                                       self.schtbl_json_max_cnt+1)
                    
                    schtbl_num += 1

                    test_flow_entity_lst = []

        elif self.db_type == 2:  # MSSQL
            # generate mssql flows
            print('=MAKING MSSQL FLOWS=')
            
            for schema_table in schemaS_tableS_lst:

                current_df = self.main_df[
                    self.main_df['schemaS.tableS'] == schema_table
                    ]

                schema_s = current_df.iloc[0]['SchemaS']
                table = current_df.iloc[0]['TableT']
                
                query_full = ''
                query_prefix = 'select '
                query_suffix = ' from $schema.$table'
                query_cast_list = []

                for _, row in current_df.iterrows():
                    attr_f = row['CodeT']
                    attr_l = row['CodeT']
                    source_column_type = row['Data Type']
                    source_column_length = ''
                    if row['Length'] and\
                        row['Data Type'].lower() not in ('smallint',
                                                        'date',
                                                        'int',
                                                        'integer'):
                        source_column_length = f"({row['Length']})"
                    else:
                        source_column_length = ''
                    query_cast_list.append(
                        f"cast('[{attr_f}]' as "
                        f"{source_column_type}{source_column_length}) as "
                        f"'[{attr_l}]'"
                        )

                query_full = ', '.join(query_cast_list)

                query_full = query_prefix + query_full + query_suffix

                flow_template = {
                    "loadType": "Scd1Replace",
                    "source": {
                        "schema": schema_s,
                        "table": table,
                        "query": query_full
                    },
                    "target": {
                        "table": table
                    }
                }

                if schtbl_cnt_trigger < self.schtbl_json_max_cnt:

                    schtbl_cnt_trigger += 1

                    test_flow_entity_lst.append(flow_template)
                
                else:
                    
                    schtbl_cnt_trigger = 0

                    test_flow_entity_lst.append(flow_template)

                    self.print_results(schema_t,
                                       test_flow_entity_lst,
                                       schtbl_num,
                                       self.schtbl_json_max_cnt+1)
                    
                    schtbl_num += 1

                    test_flow_entity_lst = []

        # for last part of batch
        if schtbl_cnt_trigger <= schtbl_len and schtbl_num > 1:
            rest_tbl_cnt = schtbl_len - (schtbl_num-1)\
                                            * (self.schtbl_json_max_cnt+1)
            self.print_results(schema_t,
                               test_flow_entity_lst,
                               schtbl_num,
                               rest_tbl_cnt)
        # if mapping table count less than self.schtbl_json_max_cnt
        if schtbl_cnt_trigger <= schtbl_len and schtbl_num == 1:
            schtbl_num = f'max_{schtbl_len}'
            self.print_results(schema_t,
                               test_flow_entity_lst,
                               schtbl_num,
                               schtbl_len)
    
    def print_results(self, schema_t, test_flow_entity_lst,
                      schtbl_num, schtbl_len):
        # print result to file
        print('=PRINT RESULT=')

        main_json_template = {
            "connection": {
                "connType": "jdbc",
                "url": "jdbc:oracle:thin:@192.168.1.67:1521:FREE",
                "driver": "oracle.jdbc.driver.OracleDriver",
                "user": "ibank2",
                "password": "ibank2"
            },
            "commonInfo": {
                "targetSchema": schema_t,
                "etlSchema": schema_t,
                "logsTable": "logs556"
            },
            "flows": test_flow_entity_lst
            }

        """
        main_json_template_default = {
            "connection": {
                "connType": "jdbc",
                "url": "...",
                "driver": "...",
                "user": "...",
                "password": "..."
            },
            "commonInfo": {
                "targetSchema": schema_t,
                "etlSchema": schema_t,
                "logsTable": "logs..."
            },
            "flows": test_flow_entity_lst
            }
        """
        
        prefix = """spark-submit \\
                --master yarn \\
                --conf spark.master=yarn \\
                --conf spark.submit.deployMode=cluster \\
                --conf spark.yarn.maxAppAttempts=1 \\
                --conf spark.sql.broadcastTimeout=600 \\
                --conf spark.hadoop.hive.exec.dynamic.partition=true \\
                --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict \\
                --conf spark.driver.userClassPathFirst=true \\
                --conf spark.executor.userClassPathFirst=true \\
                --jars /home/hdoop/drivers/jcc-11.5.9.0.jar,/home/hdoop/drivers/commons-pool2-2.11.0.jar,/home/hdoop/drivers/delta-core_2.13-2.2.0.jar,/home/hdoop/drivers/delta-storage-2.2.0.jar,/home/hdoop/drivers/mssql-jdbc-9.2.1.jre8.jar,/home/hdoop/drivers/ojdbc8-21.6.0.0.1.jar,/home/hdoop/drivers/orai18n-19.3.0.0.jar,/home/hdoop/drivers/org.apache.servicemix.bundles.kafka-clients-2.4.1_1.jar,/home/hdoop/drivers/postgresql-42.3.1.jar,/home/hdoop/drivers/spark-sql-kafka-0-10_2.13-3.3.2.jar,/home/hdoop/drivers/spark-token-provider-kafka-0-10_2.13-3.3.2.jar,/home/hdoop/drivers/vertica-jdbc-11.1.0-0.jar,/home/hdoop/drivers/xdb6-18.3.0.0.jar,/home/hdoop/drivers/xmlparserv2-19.3.0.0.jar \\
                --class sparketl.Main /home/hdoop/SparkEtl_ora.jar \\
                ' """

        suffix = " '"

        res_json = json.dumps(main_json_template)

        json_core = res_json.replace('}}', '} }').replace('{{', '{ {')\
                            .replace('}]', '} ]').replace('[{', '[ {')\
                            .replace(']}', '] }').replace('{[', '{ [')\
                            .replace('"}', '" }').replace('{"', '{ "')

        # define name for json
            
        results_file = str(self.mapping_filename.split('.')[0])

        blob_info = ''

        if self.TakeOnlyCBlobTables == 1:
            blob_info = 'cblob_only'
        elif self.IsCBlobTableIgnore == 1:
            blob_info = 'cblob_tbl_ignore'
        elif self.IsCBlobColumnIgnore == 1:
            blob_info = 'cblob_clm_ignore'
        else:
            blob_info = 'all_tables'

        results_dir = (
            f'{WORKING_DIR}/{results_file}_'
            f'{blob_info}_'
            f'{str(schtbl_num)}_{str(schtbl_len)}_'
            f'load.sh'
            )
        
        print(f'results_dir: {results_dir}')

        #
        with open(results_dir, mode="w", encoding=self.enc) as write_file:
            # json.dump(main_json_template, write_file, ensure_ascii=False)
            write_file.write(prefix)
            write_file.write(json_core)
            write_file.write(suffix)
            
        print('=DONE=')

    def run(self):
        #
        self.parse_directory()
        #
        print(f'WORKING_DIR: {WORKING_DIR}')
        print(f'dir_list: {self.dir_list}')
        print(f'Selected mapping: {self.mapping_filename}')
        #
        self.make_df()
        self.generate_json()
        

if __name__ == '__main__':
    app = App()
    app.run()
    app.pause()
