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
        self.dir_list = os.listdir(WORKING_DIR)
        self.mapping_dict = {}
        self.mapping = ''
        self.main_df = ''
        self.db_type = int(input('\nChoose DB type:\n1: Oracle\n2: MSSQL\n'))
        
    def pause(self):
        return input("Press the <ENTER> key to exit...")

    def parse_directory(self):
        
        print('=PARSING=')
        
        i = 1
        
        for file in self.dir_list:
            if file.endswith(".xlsx"):
                self.mapping_dict[i] = file
                i += 1

    def make_df(self):
        
        print('=MAKING DF=')
        
        if not self.mapping_dict:
            print('\n==================================================')
            print(f'{"!В папке отсутствует mapping файл!":^50}')
            print('==================================================')
            self.pause()
            exit()
        elif len(self.mapping_dict) == 1:
            self.mapping = next(iter(self.mapping_dict.values()))
        elif len(self.mapping_dict) > 1:
            for key in self.mapping_dict.keys():
                print(key,': ', self.mapping_dict[key])
                
            mapping_key = input('\nWrite mapping number:\n')

            self.mapping = self.mapping_dict[int(mapping_key)]

        print(f"mapping: {self.mapping}")
        
        try:
            main_df = pd.read_excel(f'{WORKING_DIR}/{self.mapping}',
                               sheet_name='Mapping',
                               usecols="D,E,G,I,J,T")
        except Exception as e:
            print(e)
            self.pause()

        main_df = main_df.drop(0,axis=0)

        main_df.columns = ['SchemaS', 'Table', 'Code', 'Data Type',
                           'Length', 'SchemaT']

        main_df = main_df[main_df['Code']!='hdp_processed_dttm']
        
        main_df = main_df.fillna('')

        main_df = main_df.sort_values(['Table'])

        main_df = main_df[main_df['Table']!='']

        main_df.index = range(1, len(main_df) + 1)
        
        self.main_df = main_df
        
        print(self.main_df.head())
        
    def generate_json(self):
        
        print('=GENERATING JSON=')

        # check db type
        if self.db_type not in (1,2):
            print('=DB CHOOSE ERROR=')
            print(f"db_type: {self.db_type}")
            self.pause()
            exit()
        #
        schema_t = self.main_df.iloc[0]['SchemaT']
        print(f'schema_t: {schema_t}')
        test_flow_entity_lst = []
        
        main_json_template = {
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
        
        # get tables from mapping
        tables_lst = self.main_df['Table'].unique()
        print(f"number of tables: {len(tables_lst)}")
        print(tables_lst)
        
        if self.db_type == 1:  # Oracle
            # generate oracle flows
            print('=MAKING ORACLE FLOWS=')
            
            for table in tables_lst:

                current_table = self.main_df[self.main_df['Table']==table]
                
                schema_s = current_table.iloc[0]['SchemaS']
                
                query_full = ''
                query_prefix = 'select '
                query_suffix = ' from $schema.$table'
                query_cast_list = []

                for _, row in current_table.iterrows():
                    attr = row['Code']
                    typ = row['Data Type']
                    length = ''
                    if row['Length'] and\
                        row['Data Type'].lower() not in ('smallint',
                                                         'date',
                                                         'int',
                                                         'integer'):
                        length = f"({row['Length']})"
                    else:
                        length = ''
                    query_cast_list.append(
                        f"cast('[{attr}]' as {typ}{length} ) as '{attr}'"
                        )

                query_full = ', '.join(query_cast_list)

                query_full = query_prefix + query_full + query_suffix

                flow_template = {
                    "loadType": "Scd1Replace",
                    "source": {
                        "schema": schema_s,
                        "table": table,
                        "query": query_full,
                        "jdbcDialect": "OracleDialect"
                    },
                    "target": {
                        "table": table
                    }
                }
                
                test_flow_entity_lst.append(flow_template)

        elif self.db_type == 2:  # MSSQL
            # generate mssql flows
            print('=MAKING MSSQL FLOWS=')
            
            for table in tables_lst:
                
                current_table = self.main_df[self.main_df['Table']==table]
                
                schema_s = current_table.iloc[0]['SchemaS']
                
                query_full = ''
                query_prefix = 'select '
                query_suffix = ' from $schema.$table'
                query_cast_list = []

                for _, row in current_table.iterrows():
                    attr = row['Code']
                    typ = row['Data Type']
                    length = ''
                    if row['Length'] and\
                        row['Data Type'].lower() not in ('smallint',
                                                         'date',
                                                         'int',
                                                         'integer'):
                        length = f"({row['Length']})"
                    else:
                        length = ''
                    query_cast_list.append(
                        f"cast([{attr}] as {typ}{length} ) as '{attr}'"
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

                test_flow_entity_lst.append(flow_template)
        
        # print result to file
        print('=PRINT RESULT=')
        
        # define name for json
        map_extr = self.mapping.split('.')
            
        results_file = str(map_extr[0])

        results_dir = (f'{WORKING_DIR}/{results_file}_load.json')
        
        print(f'results_dir: {results_dir}')
        
        with open(results_dir, mode="w", encoding="utf-8") as write_file:
            json.dump(main_json_template, write_file, ensure_ascii=False)
            
        print('=DONE=')

    def run(self):
        self.parse_directory()
        #
        print(f'WORKING_DIR: {WORKING_DIR}')
        print(f'dir_list: {self.dir_list}')
        print(f'mapping: {self.mapping}')
        #
        self.make_df()
        self.generate_json()
        

if __name__ == '__main__':
    app = App()
    app.run()
    app.pause()
