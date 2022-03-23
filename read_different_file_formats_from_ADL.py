# -*- coding: utf-8 -*-
"""
This code has funcitons that read data in python from different file formats.


"""

from azure.storage.filedatalake import DataLakeServiceClient
from azure.storage.blob import DelimitedJsonDialect,DelimitedTextDialect
from azure.storage.filedatalake._models import QuickQueryDialect
import json


def initialize_storage_account(storage_account_name, storage_account_key):
    try:  
        
        global datalake_service_client
        datalake_service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=storage_account_key)
    
    except Exception as e:
        print(e)

errors = []
def on_error(error):
       errors.append(error)
       
#Text file read from azure datalake and returned as a list (The text file had list)
def read_txt_in_list(CONTAINER,file_path):
    file_client = datalake_service_client.get_file_client(CONTAINER, file_path)
    download = file_client.download_file()
    downloaded_bytes = download.readall()
    list_systems=(downloaded_bytes.decode('utf-8')).split(",")
    
    return list_systems

#A JSON file read from the Azure Datalake, when all the contents need to be read pass param as "*"
#If want to read a specific parameter pass that parameter in the query

def read_json_ADL(CONTAINER,file_path,param):
    file_client = datalake_service_client.get_file_client(CONTAINER, file_path)
    query_expression = "SELECT "+param+" from DataLakeStorage"
    input_format = DelimitedJsonDialect(has_header=False)
    reader = file_client.query_file(query_expression, on_error=on_error, file_format=input_format)
    string_file=(reader.readall()).decode('utf-8')
    json_obj=json.loads(string_file)
    
    return json_obj[param]

#CSV file read from the azure datalake
#if entire file needs to be read, pass param as "*", else pass on the name of columns to be read or the position number of columns
def read_csv_ADL(CONTAINER,file_path,param):
    file_client = datalake_service_client.get_file_client(CONTAINER, file_path)
    query_expression = "SELECT " +param+" from DataLakeStorage"
    input_format = DelimitedTextDialect(has_header=True)
    reader = file_client.query_file(query_expression, on_error=on_error, file_format=input_format)
    content = reader.readall()
    
    return content

#Parquet file read from the datalake
#pass param as "*" when entire file needs to be read, else pass the name of the column
def read_parquet_ADL(CONTAINER,file_path,param):
    file_client = datalake_service_client.get_file_client(CONTAINER, file_path)
    query_expression = "SELECT "+param+" from DataLakeStorage"
    #input_format = QuickQueryDialect("ParquetDialect")
    reader = file_client.query_file(query_expression, on_error=on_error, file_format=QuickQueryDialect.Parquet)
    content = reader.readall()
    
    return content





