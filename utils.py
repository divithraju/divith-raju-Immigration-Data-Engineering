"""
This module is a utils module to serve different functions for the project. 

The functions are:
1) Read immigration labels
2) Quality check - record count
3) Write dataframe to parquet file
4) Write dataframe to csv file
5) Create country mapping dataframe 

"""
import time
import pandas as pd
from pyspark.sql import SparkSession


def read_immigration_labels(labels_file_path):
    """
    Read immigration labels mapping from specific file. The labels are:
        1) country mapping (country code -> country name)
        2) port mapping (port code -> port name)
        3) mode of transportation (mode code -> transportation method)
        4) address state (state code -> state name)
        5) visa type (visa code -> visa type)

    Inputs:
        labels_file_path (str): labels file full path

    Outputs:
        tuple[dict..]: tuple of immigration labels mapping dictionaries
    """
    with open(labels_file_path) as f:
        label_file_content = f.read()
        label_file_content = label_file_content.replace('\t', '')

    def code_mapper(f_content, idx):
        f_content_idx = f_content[f_content.index(idx):]
        f_content_idx = f_content_idx[:f_content_idx.index(';')].split('\n')
        f_content_idx = [i.replace("'", "") for i in f_content_idx]
        map_dic = [i.split('=') for i in f_content_idx[1:]]
        map_dic = dict([i[0].strip(), i[1].strip()] for i in map_dic if len(i) == 2)
        return map_dic

    i94cit_res = code_mapper(label_file_content, "i94cntyl")
    i94port = code_mapper(label_file_content, "i94prtl")
    i94mode = code_mapper(label_file_content, "i94model")
    i94addr = code_mapper(label_file_content, "i94addrl")
    i94visa = {'1':'Business', 
            '2': 'Pleasure',
            '3' : 'Student'}

    return i94cit_res, i94port, i94mode, i94addr, i94visa


def quality_check_count(input_df, table_name):
    """
    Apply data quality check by ensuring there are records in each table.

    Inputs:
        input_df (pyspark DataFrame): a dataframe to apply quality check on
        table_name (str): the table name
    """
    
    record_count = input_df.count()

    if record_count == 0:
        print(f"Data quality check (record counting) failed for {table_name} table with zero records")
    else:
        print(f"Data quality check (record counting) passed for {table_name} table with {record_count:,} records")


def write_to_parquet(df, output_path, table_name):
    """
    Write a dataframe as a parquet files to a specific path and table
        
    Inputs:
        df (pyspark dataframe): input dataframe
        output_path (str): the path where to store the output dataframe
        table_name (str): the table name 
        
    """
    file_path = output_path + table_name
    print(f"Writing table {table_name} to {file_path}")
    start = time.time()
    df.write.mode("overwrite").parquet(file_path)
    end = time.time()
    print(f"Write completed with {round((end-start), 2):,} sec !")


def write_to_csv(df, output_path, table_name):
    """
    Write a dataframe as a csv file to a specific path and table

    Inputs:
        df (pandas dataframe): input dataframe
        output_path (str): the path where to store the output dataframe
        table_name (str): the table name 
    """

    file_path = output_path + table_name
    print(f"Writing {table_name} to {file_path}")
    start = time.time()
    df.to_csv(f'{table_name}.csv', index=False)
    end = time.time()
    print(f"Write completed with {round((end-start), 2):,} sec !")



def create_country_mapping_df(country_dict):
    """
        Create country spark dataframe using country mapping dictionary
        
        Inputs:
            country_dict (dict): country mapping dictionary "country_code" -> "country"
            
        Outputs:
            pyspark dataframe: resulting dataframe after converting the country mapping dictionary
        
    """
    # read the dictionary
    country_mapping_df = pd.DataFrame(list(country_dict.items()), columns=['country_code', 'country_name'])

    # make the country names titlized for better reading and join
    country_mapping_df['country_name'] = country_mapping_df['country_name'].str.title()
    
    # convert country code from string to float
    country_mapping_df['country_code'] = country_mapping_df['country_code'].astype(float)

    # read the converted country mapping dataframe in spark
    spark = SparkSession.builder.\
        config("spark.jars.repositories", "https://repos.spark-packages.org/").\
        config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").\
        enableHiveSupport().getOrCreate()
    
    country_mapping_df_spark = spark.createDataFrame(country_mapping_df)
    
    return country_mapping_df_spark

    