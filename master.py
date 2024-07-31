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
        self.mapping = ''
        self.main_df = ''
        self.logs_name = str(input('\nWrite logs name:\n'))
        # results settings
        self.results_file = 'cs_load.json'
        self.results_dir = (f'{WORKING_DIR}/{self.results_file}')
        
    def pause(self):
        return input("Press the <ENTER> key to exit...")

    def parse_directory(self):
        
        print('=PARSING=')
        
        for file in self.dir_list:
            if file.endswith(".xlsx"):
                self.mapping = file

    def make_df(self):
        
        print('=MAKING DF=')
        
        if not self.mapping:
            print('\n==================================================')
            print(f'{"!В папке отсутствует mapping файл!":^50}')
            print('==================================================')
            self.pause()
            exit()
        
        try:
            main_df = pd.read_excel(f'{WORKING_DIR}/{self.mapping}',
                               sheet_name='Mapping',
                               usecols="D,T:V,Z")
        except Exception as e:
            print(e)
            self.pause()

        main_df = main_df.drop(0,axis=0)

        main_df.columns = ['SchemaS', 'SchemaT', 'Table', 'Code', 'Data Type']

        main_df = main_df[main_df['Code']!='hdp_processed_dttm']

        main_df = main_df.sort_values(['Table'])

        main_df.index = range(1, len(main_df) + 1)
        
        self.main_df = main_df
        
        print(self.main_df.head())
        
    def generate_json(self):
        
        print('=GENERATING JSON=')
        
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
                "logsTable": self.logs_name
            },
            "flows": test_flow_entity_lst
            }
        
        # get tables from mapping
        tables_lst = self.main_df['Table'].unique()
        print(f"number of tables: {len(tables_lst)}")
        print(tables_lst)
        
        # generate flows
        for table in tables_lst:
            columns = []
            columns_casts = []
            
            current_table = self.main_df[self.main_df['Table']==table]
            
            schema_s = current_table.iloc[0]['SchemaS']
            
            for _, row in current_table.iterrows():
                columns.append(row['Code'])
                columns_casts.append(
                    {
                        "name": row['Code'],
                        "colType": row['Data Type']
                    }
                )

            flow_template = {
                "loadType": "Scd1Replace",
                "source": {
                    "schema": schema_s,
                    "table": table,
                    "columns": columns,
                    "columnCasts": columns_casts,
                    "jdbcDialect": "..."
                },
                "target": {
                    "table": table
                }
            }

            test_flow_entity_lst.append(flow_template)
        
        # print json to file
        with open(self.results_dir, mode="w", encoding="utf-8") as write_file:
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
