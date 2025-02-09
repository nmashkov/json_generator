import os
from sys import exit
import pathlib

import json
import pandas as pd

from connections import oracle_vars_dict, mssql_vars_dict, local_vars_dict


BASE_DIR = str(pathlib.Path().resolve())
TARGET_DIR = ''
WORKING_DIR = f'{BASE_DIR}/{TARGET_DIR}'


class App:
    def __init__(self):
        # MAIN
        self.dir_list = os.listdir(WORKING_DIR)
        self.mapping_dict = {}
        self.mapping_filename = ''
        self.main_df = ''
        self.enc = 'utf-8'
        self.db_type = 1  # 1: Oracle, 2: MSSQL
        self.env_type = 2  # 1: Local, 2: Prod
        self.flow_type_select = 2  # 1: columnCasts, 2: Query
        self.schtbl_json_max_cnt = 25-1
        self.oracle_dt_wo_length = ('smallint', 'date', 'int', 'integer')
        self.mssql_dt_wo_length = ('image', 'ntext', 'int', 'text', 'datetime')
        # CBLOB
        self.TakeOnlyCBlobTables = 2  # 0: init select, 1: Yes, 2: No
        self.IsCBlobTableIgnore = 1   # all 0, or 1 and 2
        self.IsCBlobColumnIgnore = 2  # default: 2 1 2
        # COUNTS
        # self.source_counts_csv = ''
        # SYSTEM PARAMETERS
        self.system_number = 'test'
        self.zno_number = ''
        self.postfix_remarque = ''
        self.short_name = 1  # 1: Yes, 2: No
        self.tuz_ld = ''
        self.tuz_rd = ''  # local - user
        self.url = ''
        self.local_password = ''
        self.logs = f'logs{self.system_number}'
        # CHEATS
        self.custom_schema_s_name = ''
        self.custom_schema_t_name = ''
        self.table_type_filter = 'TableT'  # TableS TableT
        self.code_type_filter = 'CodeS'  # CodeS CodeT
        self.take_only_table_list = []
        self.ignore_table_list = []
        self.ignore_code_list = []
    
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
        
        # select Environment type
        if not self.env_type:
            self.db_type = int(input(
                '\nChoose Environment type:\n1: Local\n2: Prod\nYour choice: '
            ))

        # check Environment type
        if self.db_type not in (1,2):
            print('=ENV CHOOSE ERROR=')
            print(f"env_type: {self.env_type}")
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

        # decide print columnCasts
        if self.db_type == 1 and not self.flow_type_select:
            self.flow_type_select = int(input(
                    '\nPrint columnCasts in flows for Oracle?:'
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
                                            "V,"  # Target Code
                                            "Z,"  # Target Data Type
                                            "AA") # Target Length
        except Exception as e:
            print("Error while reading file: ", e)
            self.pause()

        main_df = main_df.drop(0,axis=0)

        main_df.columns = ['SchemaS', 'TableS', 'CodeS',
                           'DataTypeS', 'LengthS',
                           'SchemaT', 'TableT', 'CodeT',
                           'DataTypeT', 'LengthT']
        
        main_df = main_df.fillna('')

        #
        main_df['SchemaS'] = main_df['SchemaS'].apply(lambda x: str(x).strip())
        main_df['TableS'] = main_df['TableS'].apply(lambda x: str(x).strip())
        main_df['CodeS'] = main_df['CodeS'].apply(lambda x: str(x).strip())
        main_df['DataTypeS'] = main_df['DataTypeS']\
                                        .apply(lambda x: str(x).strip())
        main_df['LengthS'] = main_df['LengthS'].apply(lambda x: str(x).strip())
        #
        main_df['SchemaT'] = main_df['SchemaT'].apply(lambda x: str(x).strip())
        main_df['TableT'] = main_df['TableT'].apply(lambda x: str(x).strip())
        main_df['CodeT'] = main_df['CodeT'].apply(lambda x: str(x).strip())
        main_df['DataTypeT'] = main_df['DataTypeT']\
                                        .apply(lambda x: str(x).strip())
        main_df['LengthT'] = main_df['LengthT'].apply(lambda x: str(x).strip())

        #
        main_df = main_df[~main_df['CodeT'].isin(['hdp_processed_dttm'])]
        main_df = main_df[main_df['CodeT']!='']
        
        # CBLOB
        if self.IsCBlobTableIgnore == 1:
            cblob_table_df = main_df[
                    main_df['DataTypeS'].isin(['CLOB', 'BLOB'])
                ]
            ignore_cblob_table_list = cblob_table_df['TableS'].unique().tolist()
            if ignore_cblob_table_list:
                main_df = main_df[~main_df['TableS']\
                                    .isin(ignore_cblob_table_list)]
        elif self.TakeOnlyCBlobTables == 1:
            cblob_table_df = main_df[
                    main_df['DataTypeS'].isin(['CLOB', 'BLOB'])
                ]
            cblob_table_list = cblob_table_df['TableS'].unique().tolist()
            if cblob_table_list:
                main_df = main_df[main_df['TableS']\
                                    .isin(cblob_table_list)]
            if self.IsCBlobColumnIgnore == 1:
                main_df = main_df[~main_df['DataTypeS'].isin(['CLOB', 'BLOB'])]
        elif self.IsCBlobColumnIgnore == 1:
            main_df = main_df[~main_df['DataTypeS'].isin(['CLOB', 'BLOB'])]
        
        # CHEAT
        if self.custom_schema_s_name:
            main_df['SchemaS'] = self.custom_schema_s_name
        elif self.env_type == 1:
            main_df['SchemaS'] = local_vars_dict['custom_schema_s_name']
        
        if self.custom_schema_t_name:
            main_df['SchemaT'] = self.custom_schema_t_name
        elif self.env_type == 1:
            main_df['SchemaT'] = local_vars_dict['custom_schema_t_name']

        if self.take_only_table_list:
            main_df = main_df[main_df[self.table_type_filter].isin(self.take_only_table_list)]

        if self.ignore_table_list:
            main_df = main_df[~main_df[self.table_type_filter].isin(self.ignore_table_list)]

        if self.ignore_code_list:
            main_df = main_df[~main_df[self.code_type_filter].isin(self.ignore_code_list)]
        
        #
        main_df['schemaS.tableS'] = main_df['SchemaS'] +'.'+ main_df['TableS']

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

        # generate flows
        if self.db_type == 1:  # Oracle
            print_db_type = 'ORACLE'
        elif self.db_type == 2:  # MSSQL
            print_db_type = 'MSSQL'
        print(f'=MAKING {print_db_type} FLOWS=')
        
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

            columns_casts = []

            for _, row in current_df.iterrows():
                target_column_name = row['CodeT']
                source_column_name = row['CodeS']
                target_column_type = row['DataTypeT']
                source_column_type = row['DataTypeS']
                target_column_length = ''
                source_column_length = ''
                
                if self.db_type == 1:  # Oracle
                    if row['LengthS'] and\
                        row['DataTypeS'].lower() not in self.oracle_dt_wo_length:
                        source_column_length = f"({row['LengthS']})"
                    elif row['DataTypeS'].lower() == 'varchar2':
                        source_column_length = '(4000)'
                    else:
                        source_column_length = ''
                        
                    if row['LengthT'] and\
                        row['DataTypeT'].lower()=='decimal':
                        target_column_length = f"({row['LengthT']})"
                    else:
                        target_column_length = ''
                elif self.db_type == 2:  # MSSQL
                    if row['LengthS'] and\
                        row['DataTypeS'].lower() not in self.mssql_dt_wo_length:
                        source_column_length = f"({row['LengthS']})"
                    else:
                        source_column_length = ''
                        
                    if row['LengthT'] and\
                        row['DataTypeT'].lower() not in self.mssql_dt_wo_length:
                        target_column_length = f"({row['LengthT']})"
                    else:
                        target_column_length = ''

                if self.flow_type_select == 1:
                    columns_casts.append(
                        {
                            "name": source_column_name,
                            "colType": f"{target_column_type}{target_column_length}"
                        }
                    )
                elif self.flow_type_select == 2:
                    if self.db_type == 1:  # Oracle
                        query_cast_list.append(
                            f"cast('{source_column_name}' as "
                            f"{source_column_type}{source_column_length}) as "
                            f"'{target_column_name}'"
                        )
                    elif self.db_type == 2:  # MSSQL
                        query_cast_list.append(
                            f"cast('[{source_column_name}]' as "
                            f"{source_column_type}{source_column_length}) as "
                            f"'[{target_column_name}]'"
                        )

            query_full = ', '.join(query_cast_list)

            query_full = query_prefix + query_full + query_suffix

            flow_template = {}

            if self.db_type == 1:  # Oracle
                if self.flow_type_select == 1:
                    # columnCasts
                    flow_template = {
                        "loadType": "Scd1Replace",
                        "source": {
                            "schema": schema_s,
                            "table": source_table,
                            "columnCasts": columns_casts,
                            "jdbcDialect": "OracleDialect"
                        },
                        "target": {
                            "table": target_table
                        }
                    }
                elif self.flow_type_select == 2:
                    # Query
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
            elif self.db_type == 2:  # MSSQL
                if self.flow_type_select == 1:
                    # columnCasts
                    flow_template = {
                        "loadType": "Scd1Replace",
                        "source": {
                            "schema": schema_s,
                            "table": '['+source_table+']',
                            "columnCasts": columns_casts
                        },
                        "target": {
                            "table": target_table
                        }
                    }
                elif self.flow_type_select == 2:
                    # Query
                    flow_template = {
                        "loadType": "Scd1Replace",
                        "source": {
                            "schema": schema_s,
                            "table": '['+source_table+']',
                            "query": query_full
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
            schtbl_num = '1'
            self.print_results(schema_t,
                               test_flow_entity_lst,
                               schtbl_num,
                               schtbl_len)
    
    def print_results(self, schema_t, test_flow_entity_lst,
                      schtbl_num, schtbl_len):
        # print result to file
        print('=PRINT RESULT=')

        main_json_template = {}
        
        print_tuz_rd = ''
        print_tuz_ld = ''
        print_url = ''
        print_logs = ''
        print_local_password = ''

        if self.tuz_rd:
            print_tuz_rd = self.tuz_rd
        elif self.env_type == 1:
            if self.db_type == 1:
                print_tuz_rd = oracle_vars_dict['tuz_rd']
            else:
                print_tuz_rd = mssql_vars_dict['tuz_rd']
        else:
            print_tuz_rd = 'TODO_TUZ_RD'
        if self.tuz_ld:
            print_tuz_ld = self.tuz_ld
        else:
            print_tuz_ld = 'TODO_TUZ_LD'
        if self.url:
            print_url = self.url
        elif self.env_type == 1:
            if self.db_type == 1:
                print_url = oracle_vars_dict['url']
            else:
                print_url = mssql_vars_dict['url']
        else:
            print_url = 'TODO_URL'
        if self.logs:
            print_logs = self.logs
        else:
            print_logs = 'TODO_LOGS'

        #
        print_driver = ''
        if self.db_type == 1:
            print_driver = 'oracle.jdbc.driver.OracleDriver'
        elif self.db_type == 2:
            print_driver = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'

        main_json_template = {
            "connection": {
                "connType": "jdbc",
                "url": "###connection.url###",
                "driver": f"{print_driver}",
                "user": "###connection.user###",
                "password": "###connection.password###"
            },
            "commonInfo": {
                "targetSchema": schema_t,
                "etlSchema": schema_t,
                "logsTable": f"{print_logs}"
            },
            "flows": test_flow_entity_lst
            }

        # define name for json and application

        if self.system_number:
            print_system_number = self.system_number
        else:
            print_system_number = str(self.mapping_filename.split('.')[0])

        env_db_info = ''

        if self.env_type == 1:  # Local
            env_db_info = 'local_'
            if self.db_type == 1:  # Oracle
                env_db_info += 'oracle'
            elif self.db_type == 2:  # MSSQL
                env_db_info += 'mssql'
        elif self.env_type == 2:  # Prod
            env_db_info = 'prod_'
            if self.db_type == 1:  # Oracle
                env_db_info += 'oracle'
            elif self.db_type == 2:  # MSSQL
                env_db_info += 'mssql'
                
        blob_info = ''

        if self.TakeOnlyCBlobTables == 1:
            blob_info = 'cblob_only'
        elif self.IsCBlobTableIgnore == 1:
            blob_info = 'cblob_tbl_ignore'
        elif self.IsCBlobColumnIgnore == 1:
            blob_info = 'cblob_clm_ignore'
        else:
            blob_info = 'all_tables'

        columnCasts_info = ''

        if self.flow_type_select == 1:
            columnCasts_info = 'columnCasts_'
        else:
            columnCasts_info = ''
        
        if self.zno_number:
            print_zno_number = self.zno_number
        else:
            print_zno_number = 'N'
            
        if self.postfix_remarque:
            print_postfix_remarque = '_'+self.postfix_remarque
        else:
            print_postfix_remarque = ''
        
        print_load_name = ''

        if self.short_name == 1:  # Yes
            print_load_name = (
                f'{print_system_number}_'
                f'{print_zno_number}_'
                f'load_'
                f'{str(schtbl_len)}_'
                f'{str(schtbl_num)}'
                f'{print_postfix_remarque}'
            )
        else:  # No
            print_load_name = (
                f'{print_system_number}_'
                f'{print_zno_number}_'
                f'load_'
                f'{env_db_info}_'
                f'{blob_info}_'
                f'{columnCasts_info}'
                f'{str(schtbl_len)}_'
                f'{str(schtbl_num)}'
                f'{print_postfix_remarque}'
            )
        
        # form final json
        
        if self.local_password:
            print_local_password = self.local_password
        elif self.env_type == 1:
            if self.db_type == 1:
                print_local_password = oracle_vars_dict['local_password']
            else:
                print_local_password = mssql_vars_dict['local_password']
        else:
            print_local_password = '\"TODO_PW\"' 
        
        prefix_local_oracle = f"""spark-submit --name {print_load_name} --conf spark.connection.url=\"{print_url}\" --conf spark.connection.user=\"{print_tuz_rd}\" --conf spark.connection.password=\"{print_local_password}\" --master yarn --conf spark.master=yarn --conf spark.submit.deployMode=cluster --conf spark.yarn.maxAppAttempts=1 --conf spark.sql.broadcastTimeout=600 --conf spark.hadoop.hive.exec.dynamic.partition=true --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict --conf spark.driver.userClassPathFirst=true --conf spark.executor.userClassPathFirst=true --conf spark.driver.extraJavaOptions=-Duser.timezone=Europe/Moscow --conf spark.executor.extraJavaOptions=-Duser.timezone=Europe/Moscow --jars /home/hdoop/drivers/ojdbc8-21.6.0.0.1.jar,/home/hdoop/drivers/orai18n-19.3.0.0.jar --class sparketl.Main /home/hdoop/SparkEtl_v2.jar ' """
        
        prefix_local_mssql = f"""spark-submit --name {print_load_name} --conf spark.connection.url=\"{print_url}\" --conf spark.connection.user=\"{print_tuz_rd}\" --conf spark.connection.password=\"{print_local_password}\" --master yarn --conf spark.master=yarn --conf spark.submit.deployMode=cluster --conf spark.yarn.maxAppAttempts=1  --conf spark.sql.broadcastTimeout=600 --conf spark.hadoop.hive.exec.dynamic.partition=true --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict --conf spark.driver.userClassPathFirst=true --conf spark.executor.userClassPathFirst=true --conf spark.driver.extraJavaOptions=-Duser.timezone=Europe/Moscow --conf spark.executor.extraJavaOptions=-Duser.timezone=Europe/Moscow --jars /home/hdoop/drivers/mssql-jdbc-9.2.1.jre8.jar --class sparketl.Main /home/hdoop/SparkEtl_v2.jar ' """

        prefix_prod_oracle = f"""spark3-submit \\\n--keytab ~/{print_tuz_ld}.keytab \\\n--principal {print_tuz_ld}@REGION.VTB.RU \\\n--name {print_load_name} \\\n--conf spark.connection.url=\"{print_url}\" \\\n--conf spark.connection.user=\"{print_tuz_rd}\" \\\n--conf spark.connection.password=\"TODO_PW\" \\\n--master yarn \\\n--conf spark.master=yarn \\\n--conf spark.submit.deployMode=cluster \\\n--conf spark.yarn.maxAppAttempts=1 \\\n--conf spark.dynamicAllocation.enabled=False \\\n--conf spark.driver.memory=3g \\\n--conf spark.executor.memory=2g \\\n--conf spark.executor.cores=4 \\\n--conf spark.executor.instances=6 \\\n--conf spark.executor.memoryOverhead=4g \\\n--conf spark.hadoop.hive.exec.dynamic.partition=True \\\n--conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict \\\n--conf spark.sql.parquet.int96RebaseModeInWrite=LEGACY \\\n--conf spark.sql.parquet.int96RebaseModeInRead=LEGACY \\\n--conf spark.driver.extraJavaOptions=-Duser.timezone=Europe/Moscow \\\n--conf spark.executor.extraJavaOptions=-Duser.timezone=Europe/Moscow \\\n--jars hdfs:///apps/sparkjars/2239/ojdbc8.jar,hdfs:///apps/sparkjars/2239/orai18n.jar \\\n--class sparketl.Main \\\n--deploy-mode cluster hdfs:///apps/sparkjars/2239/SparkEtl_v2.jar \\\n' """

        prefix_prod_mssql = f"""spark3-submit \\\n--keytab ~/{print_tuz_ld}.keytab \\\n--principal {print_tuz_ld}@REGION.VTB.RU \\\n--name {print_load_name} \\\n--conf spark.connection.url=\"{print_url}\" \\\n--conf spark.connection.user=\"{print_tuz_rd}\" \\\n--conf spark.connection.password=\"TODO_PW\" \\\n--master yarn \\\n--conf spark.master=yarn \\\n--conf spark.submit.deployMode=cluster \\\n--conf spark.yarn.maxAppAttempts=1 \\\n--conf spark.dynamicAllocation.enabled=False \\\n--conf spark.driver.memory=3g \\\n--conf spark.executor.memory=2g \\\n--conf spark.executor.cores=4 \\\n--conf spark.executor.instances=6 \\\n--conf spark.executor.memoryOverhead=4g \\\n--conf spark.hadoop.hive.exec.dynamic.partition=True \\\n--conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict \\\n--conf spark.sql.parquet.int96RebaseModeInWrite=LEGACY \\\n--conf spark.sql.parquet.int96RebaseModeInRead=LEGACY \\\n--conf spark.driver.extraJavaOptions=-Duser.timezone=Europe/Moscow \\\n--conf spark.executor.extraJavaOptions=-Duser.timezone=Europe/Moscow \\\n--jars hdfs:///apps/sparkjars/2239/mssql-jdbc-9.2.1.jre8.jar \\\n--class sparketl.Main \\\n--deploy-mode cluster hdfs:///apps/sparkjars/2239/SparkEtl_v2.jar \\\n' """

        if self.env_type == 1:  # Local
            # prefix = prefix_local
            if self.db_type == 1:  # Oracle
                prefix = prefix_local_oracle
            elif self.db_type == 2:  # MSSQL
                prefix = prefix_local_mssql
        elif self.env_type == 2:  # Prod
            if self.db_type == 1:  # Oracle
                prefix = prefix_prod_oracle
            elif self.db_type == 2:  # MSSQL
                prefix = prefix_prod_mssql

        suffix = " '"

        res_json = json.dumps(main_json_template)

        json_core = res_json.replace('}}', '} }').replace('{{', '{ {')\
                            .replace('}]', '} ]').replace('[{', '[ {')\
                            .replace(']}', '] }').replace('{[', '{ [')\
                            .replace('"}', '" }').replace('{"', '{ "')

        results_dir = (
            f'{WORKING_DIR}/'
            f'{print_load_name}'
            f'.sh'
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
    # app.pause()
