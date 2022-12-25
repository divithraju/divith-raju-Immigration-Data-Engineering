"""
This module is used to clean tables and datasets, such nulls and duplicates, to prepare it for the data model. 

The tables are:
- Immigration dataset
- Temperature dataset
- U.S demographic dataset
- Airport dataset

"""
from pyspark.sql.functions import isnan, when, count, col, upper
import pandas as pd
import numpy as np
import re


def drop_nulls_duplicates(df):
    """
    Drop columns with more than 90% null values, drop duplicate rows, and drop rows where its values all are missing using pandas.
    
    Inputs:
        df (pd.DataFrame): a pandas dataframe
    
    Outputs:
        pd.DataFrame: cleaned datafeame
    
    """
    # percentage of null values
    perc_nulls = round((df.isna().sum() / df.shape[0]) * 100, 2)
    print("Percentage of null values:")
    print(perc_nulls.to_string())
    print()
    
    # drop columns with more than 90% null values 
    drop_cols = list(perc_nulls[perc_nulls > 90].index)
    if len(drop_cols) == 0:
        print("No columns to be dropped")
    else:
        print("Columns to be dropped:", drop_cols)
    
    clean_df = df.drop(columns=drop_cols)
    print()
        
    # drop duplicate rows
    num_duplicate = clean_df.duplicated().sum()
    print("Number of duplicate rows:", num_duplicate)
    print()
    clean_df = clean_df.drop_duplicates()
    
    # drop rows where its values are missing
    num_rows_all_miss = clean_df[clean_df.isnull().all(axis=1)].shape[0]
    print("Number of rows where all values are missing:", num_rows_all_miss)
    if num_rows_all_miss > 0:
        print("Drop rows where all values are missing")
    
    print()
    clean_df = clean_df.dropna(how='all')
    
    # drop miss index columns (exploration appears: ['Unnamed: 0'])
    miss_index_cols = [col for col in list(clean_df.columns) if 'unnamed' in col.lower()]
    print("Drop miss index columns if exist (Unnamed):", miss_index_cols)
    clean_df = clean_df.drop(miss_index_cols, axis=1)
    
    return clean_df


def drop_nulls_duplicates_spark(df):
    """
    drop columns with more than 90% null values, drop duplicate rows, and drop rows where its values all are missing using spark.
    
    Inputs:
        df (pyspark DataFrame): a pyspark dataframe
    
    Outputs:
        pyspark DataFrame: cleaned datafeame
    
    """
    # percentage of null values
    perc_nulls = ((df.select([count(when(col(c).contains('None') | \
                            col(c).contains('NULL') | \
                            (col(c) == '') | \
                            col(c).isNull() | \
                            isnan(c), c 
                           )).alias(c)
                    for c in df.columns]).toPandas() / df.count()) * 100)

    perc_nulls = pd.melt(perc_nulls, var_name='col_name', value_name='perc')
    perc_nulls = perc_nulls.set_index(perc_nulls.columns[0]).squeeze()
    print("Percentage of null values:")
    print(perc_nulls.to_string())
    print()
    
    # drop columns with more than 90% null values (exploration appears: ['occup', 'entdepu', 'insnum'])
    drop_cols = list(perc_nulls[perc_nulls > 90].index)
    if len(drop_cols) == 0:
        print("No columns to be dropped")
    else:
        print("Columns to be dropped:", drop_cols)
    
    clean_df = df.drop(*drop_cols)
    print()
        
    # drop duplicate rows
    num_records_before_dup = clean_df.count()
    clean_df = clean_df.dropDuplicates()
    num_records_after_dup = clean_df.count()
    num_duplicate = num_records_before_dup - num_records_after_dup
    print("Number of duplicate rows:", num_duplicate)
    print()
    
    # drop rows where its values are missing
    clean_df = clean_df.dropna(how='all')
    num_records_after_row_miss = clean_df.count()
    num_rows_all_miss = num_records_after_dup - num_records_after_row_miss
    print("Number of rows where all values are missing:", num_rows_all_miss)
    if num_rows_all_miss > 0:
        print("Drop rows where all values are missing")
        
    # drop miss index columns
    miss_index_cols = [col for col in list(clean_df.columns) if 'unnamed' in col.lower()]
    print("\nDrop miss index columns if exist (Unnamed):", miss_index_cols)
    clean_df = clean_df.drop(*drop_cols)
    
    return clean_df


def clean_immigration_table(input_df, process_spark=False):
    """
    Clean the immigration table using spark or pandas.

    Inputs:
        input_df (dataframe): pandas or pyspark dataframe (process_spark argument changes accordingly)
        process_spark (bool, optional): whether to process cleaning the input dataframe using spark or pandas. Defaults to False.

    Outputs:
        pyspark/pandas dataframe: cleaned immigration dataframe
    """
    
    # process the cleaning using spark or pandas based on the flag parameter and input dataframe type
    # data exploration appears that these columns will be dropped due null values problems: ['occup', 'entdepu', 'insnum'])
    # make sure that i94addr state name are all capitalized 
    if process_spark:
        clean_immigration_df = drop_nulls_duplicates_spark(input_df) 
        clean_immigration_df = clean_immigration_df.select("*", upper("i94addr"))

    else:
        # data exploration appears that these columns will be dropped due to miss indexing problems: ['Unnamed: 0'])
        clean_immigration_df = drop_nulls_duplicates(input_df)
        clean_immigration_df['i94addr'] = clean_immigration_df['i94addr'].str.upper()

    return clean_immigration_df


def clean_temperature_table(input_df):
    """
    Clean the temperature table.

    Inputs:
        input_df (pd.DataFrame): input dataframe

    Outputs:
        pd.DataFrame: cleaned temperature dataframe
    """
    # create copy of input dataframe for cleaning
    clean_temperature_df = input_df.copy()

    # convert dt column type from object type to datetime type
    clean_temperature_df['dt'] = pd.to_datetime(clean_temperature_df['dt'])

    # get only temperature from 2010 and above
    clean_temperature_df = clean_temperature_df[clean_temperature_df['dt'] >= '2010-01-01']

    # titleize the country to unify the the country naming 
    clean_temperature_df['Country'] = clean_temperature_df['Country'].str.title()

    # get only rows with non-null values of temperature
    clean_temperature_df = clean_temperature_df.dropna(subset=['AverageTemperature', 'AverageTemperatureUncertainty'], how='all')

    # check for nulls and duplicates
    clean_temperature_df = drop_nulls_duplicates(clean_temperature_df)

    # convert Latitude & Longitude columns to numerical values
    def convert_to_numeric_coord(coord):
        num = re.findall("\d+\.\d+", coord)
        if coord[-1] in ["W", "S"]:
            return -1. * float(num[0])
        else:
            return float(num[0])

    clean_temperature_df['Latitude'] = clean_temperature_df['Latitude'].apply(convert_to_numeric_coord)
    clean_temperature_df['Longitude'] = clean_temperature_df['Longitude'].apply(convert_to_numeric_coord)

    return clean_temperature_df


def clean_demographics_table(input_df):
    """
    Clean the demographics table.

    Inputs:
        input_df (pd.DataFrame): input dataframe

    Outputs:
        pd.DataFrame: cleaned demographics dataframe
    """
    # check for nulls and duplicated for demographics_df
    clean_demographics_df = drop_nulls_duplicates(input_df)
    
    # make sure that state code are all capitalized
    clean_demographics_df['State Code'] = clean_demographics_df['State Code'].str.upper()
    
    return clean_demographics_df
    

def clean_airport_table(input_df):
    """
    Clean the airport table.

    Inputs:
        input_df (pd.DataFrame): input dataframe

    Outputs:
        pd.DataFrame: cleaned airport dataframe
    """

    # create copy of input dataframe for cleaning
    clean_airport_df = input_df.copy()
    
    # make the airport name titleized
    clean_airport_df['name'] = clean_airport_df['name'].str.title()
    
    # convert the value '0, 0' in coordinates column to nan, since '0, 0' is origin coordinates - above the ocean (data exploration)
    clean_airport_df.loc[clean_airport_df['coordinates'] == '0, 0', 'coordinates'] = np.nan

    # convert the value '0' in iata_code column to nan, since there is no '0' as iata_code (data exploration)
    clean_airport_df.loc[clean_airport_df['iata_code'] == '0', 'iata_code'] = np.nan

    # check for nulls and duplicated for airport_df
    clean_airport_df = drop_nulls_duplicates(clean_airport_df)

    # convert coordinates column to longitude & latitude columns with float data type
    clean_airport_df[['longitude', 'latitude']] = clean_airport_df['coordinates'].apply(lambda x: pd.Series(list(map(float, map(str.strip, x.split(","))))) \
                                                                                        if x is not np.nan else pd.Series([np.nan, np.nan]))

    # drop coordinates column
    clean_airport_df = clean_airport_df.drop(['coordinates'], axis=1)

    return clean_airport_df
