import re
from os.path import join

import pandas as pd
from sqlalchemy import types
import sqlalchemy
from sqlalchemy import Column, DateTime, Index, Integer, MetaData, Text, BOOLEAN, VARCHAR, SmallInteger,TIMESTAMP,TIME
from airflow.exceptions import AirflowFailException
from supportfirst.utils import get_data_field_name, engine
import openpyxl

class LoadTempTable:

    def __init__(self, table_name, file_root_path, file_path, data_fields, text_analysis_column, data_map_id, master_table_name, _logging) -> None:
        self.table_name = table_name
        self.file_root_path = file_root_path
        self.file_path = file_path
        self.replace_column = {}
        self.data_fields = data_fields
        self.df = None
        self.data_type_dict = {}
        text_analysis_column_list = text_analysis_column
        if text_analysis_column_list:
            self.text_analysis_column = [text_analysis_column[0].lower()]
        else:
            self.text_analysis_column = None
        self.data_map_id = data_map_id
        self.master_table_name = master_table_name
        self._logging = _logging

    def get_data_column(self, field_type):
        self._logging.info("in load temp table get_data_column " + str(field_type))

        if field_type == 'text':
            self._logging.info("in load temp table get_data_column  text field " + str(field_type))
            return types.Text()
        elif field_type == "time without time zone":
            # self._logging.info("in ------------- get_data_column temp table  Time data type->" + str(field_type))
            #return Column(TIME(False))
            return types.Time(False)
            

        elif field_type == "date" or \
            field_type == "timestamp without time zone":
            self._logging.info("in load temp table date and timestamp without time zone" + str(field_type))
            #return Column(TIMESTAMP(False))
            return sqlalchemy.DateTime()
            
            #return sqlalchemy.DateTime(timezone=False)
            #return types.Text()
        elif field_type == "smallint":
            return types.Integer()
        elif field_type == "integer":
            return types.Integer()
        elif field_type == "character varying":
            return types.Text()
        elif field_type == "timestamp with time zone":
            self._logging.info("in load temp table timestamp with time zone field --> " + str(field_type))
            return sqlalchemy.DateTime(timezone=True)
        elif field_type == "time with time zone":
            self._logging.info("in load temp table time with time zone field --> " + str(field_type))
            return sqlalchemy.DateTime(timezone=True)
    
    # def get_field_backend_name(self, field_dict, field_name):
    #     print("data_fields: ",field_dict["field_name"].lower(),"column name: ", field_name.lower(), field_dict["field_name"].lower() == field_name.lower())
    #     print(type(field_name))
    #     print(type(field_dict["field_name"]))
    #     return field_dict["field_backend_name"]

    # def get_corresponding_col_backend_name(self, col):
    #     for field in self.data_fields:
    #         res = self.get_field_backend_name(field, col)
    #         if res:
    #             return res
    
    # def get_backend_field_name_direct(self, col):
    #     print("****************000000000000000000000000000000008888888888888888888")
    #     print(len(self.data_fields))

    def sanitize_df(self, df):
        df_columns = df.columns
        santized_columns = {}
        for column in df_columns:
            column_ = column.strip()
            column_ = column.strip(u'\u200b').strip()
            santized_columns.update({column: column_})
        df = df.rename(columns = santized_columns)
        return df
        

    def _sanitize_column(self, columns):
        for col, data_field in zip(columns, self.data_fields):
                col_to_be_replaced = data_field["field_backend_name"].lower()
                if self.text_analysis_column:
                    if data_field["field_name"].lower() in self.text_analysis_column:
                        col_to_be_replaced = col_to_be_replaced+"_col"
                self.replace_column.update({col: col_to_be_replaced})
                self.data_type_dict[col_to_be_replaced] = self.get_data_column(data_field["field_type"])
        self.df = self.df.rename(columns=self.replace_column)

    def get_required_columns(self):
        column = ""
        required_columns = []
        for data_field in self.data_fields:
            column = data_field['field_name'].strip()
            column = column.strip(u'\u200b').strip()
            required_columns.append(column)
        return required_columns

    def load_file_to_dataframe(self):
        try:
            req_column = self.get_required_columns()
            if self.file_path.endswith(("xls", "xlsx", "xlsm", "xlsb", "odf", "ods", "odt")):
                if self.file_path.endswith(("xls")):
                    self.df = pd.read_excel(join(self.file_root_path, self.file_path), engine="xlrd")
                    self.df = self.sanitize_df(self.df)
                    self.df = self.df[req_column]
                else:
                    self.df = pd.read_excel(join(self.file_root_path, self.file_path), engine="openpyxl")
                    self.df = self.sanitize_df(self.df)
                    self.df = self.df[req_column]
            else:
                try:
                    self.df = pd.read_csv(join(self.file_root_path, self.file_path), encoding = 'unicode_escape', engine ='python')
                    self.df = self.sanitize_df(self.df)
                    self.df = self.df[req_column]
                except UnicodeDecodeError as e:
                    self.df = pd.read_csv(join(self.file_root_path, self.file_path), engine ='python')
                    self.df = self.sanitize_df(self.df)
                    self.df = self.df[req_column]
                # else:
                #     raise(UnicodeDecodeError)
        except KeyError as e:
            self._logging.error("The given field_name is not matching with the file columns. {}".format(e))
            raise AirflowFailException("column: {} not present".
                                                format(e))

        self._sanitize_column(self.df.columns)
        self.df["analysis_base_id"] = self.data_map_id
        self._remove_duplicate()
        
    def _remove_duplicate(self):
        self.df.drop_duplicates(keep="first", inplace=True)
    
    def validate(self,):
        for item in self.data_fields:
            col = str.lower(get_data_field_name(item))
            if col not in [*self.replace_column.values()] \
                    and col not in [*self.replace_column]:
                if self.text_analysis_column and col not in self.text_analysis_column:
                    text_col = self.text_analysis_column[0]
                    p = re.compile(r"{}".format(text_col), re.I)
                    if not p.search(text_col):
                        self._logging.error("Exception occured when loading data to Temporary Table")
                        raise AirflowFailException("Field: {} not present".
                                                format(col))



    def load_table(self):
        try:
            self.df.to_sql(
                self.table_name,
                con=engine.connect(),
                if_exists='append',
                index=False,
                dtype = self.data_type_dict
                
            )
        except:
            self._logging.error("Exception occured when loading data to Temporary Table")
            raise AirflowFailException("Data level issue please check the data source and upload again")
                                                

