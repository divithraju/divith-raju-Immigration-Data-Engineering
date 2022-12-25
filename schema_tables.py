"""
This module is used to create fact and dimension tables (schema tables) and write them into parquet files.

The tables are:
- Fact table:
1) Immigration table
- Dimension tables:
1) Migrant table
2) Status table
3) Visa table
4) Demographics table
5) Airport table
6) Country Temperature table
7) Date table

"""
from pyspark.sql import functions as F
from pyspark.sql.functions import col, monotonically_increasing_id, udf
from pyspark.sql.types import DateType
from pyspark.sql.functions import year, month, dayofmonth, dayofweek
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

from utils import write_to_parquet, create_country_mapping_df

def create_migrant_dimension(input_df, output_path, table_name):
    """
        Create Migrant dimension, model its data, and write the data into parquet files.
        
        Inputs:
            input_df (pyspark dataframe): input dataframe
            output_path (str): the path where to store the output dataframe
            table_name (str): the table name 
            
        Outputs:
            pyspark dataframe: a dataframe representing migrant dimension
    """
    # create the dimension table
    df = input_df.withColumn("migrant_id", monotonically_increasing_id()) \
                .select(["migrant_id", "biryear", "gender"]) \
                .withColumnRenamed("biryear", "birth_year")\
                .dropDuplicates(["birth_year", "gender"])
    
    
    # write the table to parquet file
    write_to_parquet(df, output_path, table_name)
    
    return df


def create_status_dimension(input_df, output_path, table_name):
    """
        Create Status dimension, model its data, and write the data into parquet files.
        
        Inputs:
            input_df (pyspark dataframe): input dataframe
            output_path (str): the path where to store the output dataframe
            table_name (str): the table name 
            
        Outputs:
            pyspark dataframe: a dataframe representing flag dimension
    """
    # create the dimension table
    df = input_df.withColumn("status_flag_id", monotonically_increasing_id()) \
                .select(["status_flag_id", "entdepa", "entdepd", "matflag"]) \
                .withColumnRenamed("entdepa", "arrival_flag")\
                .withColumnRenamed("entdepd", "departure_flag")\
                .withColumnRenamed("matflag", "match_flag")\
                .dropDuplicates(["arrival_flag", "departure_flag", "match_flag"])
    
    
    # write the table to parquet file
    write_to_parquet(df, output_path, table_name)
    
    return df

def create_visa_dimension(input_df, output_path, table_name):
    """
        Create Visa dimension, model its data, and write the data into parquet files.
        
        Inputs:
            input_df (pyspark dataframe): input dataframe
            output_path (str): the path where to store the output dataframe
            table_name (str): the table name 
            
        Outputs:
            pyspark dataframe: a dataframe representing visa dimension
    """
    # create the dimension table
    df = input_df.withColumn("visa_id", monotonically_increasing_id()) \
                .select(["visa_id","i94visa", "visatype", "visapost"]) \
                .dropDuplicates(["i94visa", "visatype", "visapost"])
    
    
    # write the table to parquet file
    write_to_parquet(df, output_path, table_name)
    
    return df


def create_demographics_dimension(input_df, output_path, table_name):
    """
        Create demographics dimension, model its data, and write the data into parquet files.
        
        Inputs:
            input_df (pyspark dataframe): input dataframe
            output_path (str): the path where to store the output dataframe
            table_name (str): the table name 
            
        Outputs:
            pyspark dataframe: a dataframe representing state_demographics dimension
    """
    # create the dimension table
    df = input_df.select(["State Code", "State", "Median Age", "Male Population", "Female Population", "Total Population", \
                          "Average Household Size", "Foreign-born"])\
                .withColumnRenamed("State Code", "state_code")\
                .withColumnRenamed("Median Age", "median_age")\
                .withColumnRenamed("Male Population", "male_population")\
                .withColumnRenamed("Female Population", "female_population")\
                .withColumnRenamed("Total Population", "total_population")\
                .withColumnRenamed("Average Household Size", "average_household_size")\
                .withColumnRenamed("Foreign-born", "foreign_born")
    
    # group by state to join it with the fact table (i94addr - USA State of arrival) and observe relevant information
    df = df.groupBy(col("state_code"), col("State").alias("state")).agg(
                F.round(F.mean('median_age'), 2).alias("median_age"),\
                F.sum("total_population").alias("total_population"),\
                F.sum("male_population").alias("male_population"), \
                F.sum("female_population").alias("female_population"),\
                F.sum("foreign_born").alias("foreign_born"), \
                F.round(F.mean("average_household_size"), 2).alias("average_household_size")
                ).dropna(how='all', subset=['median_age', 'total_population', 'male_population', 'female_population', 'foreign_born', 'average_household_size'])
    
    
    # write the table to parquet file
    write_to_parquet(df, output_path, table_name)
    
    return df


def create_airport_dimension(input_df, output_path, table_name):
    """
        Create Airport dimension, model its data, and write the data into parquet files.
        
        Inputs:
            input_df (pyspark dataframe): input dataframe
            output_path (str): the path where to store the output dataframe
            table_name (str): the table name 
            
        Outputs:
            pyspark dataframe: a dataframe representing airport dimension
    """
    # create the dimension table
    df = input_df.select(["iata_code", "ident", "type", "name", "elevation_ft", "continent", "iso_country", "iso_region", \
                          "municipality", "gps_code", "local_code"])\
                .dropDuplicates(["iata_code"])
    
    
    # write the table to parquet file
    write_to_parquet(df, output_path, table_name)
    
    return df


def create_country_temperature_dimension(country_dict, input_df, output_path, table_name):
    """
        Create Country Temperature dimension, model its data, and write the data into parquet files.
        
        Inputs:
            country_dict (dict): country mapping dictionary "country_code" -> "country"
            input_df (pyspark dataframe): input dataframe
            output_path (str): the path where to store the output dataframe
            table_name (str): the table name 
            
        Outputs:
            pyspark dataframe: a dataframe representing temperature dimension
    """
    # create the dimension table
    # 1) create country mapping dataframe to join with country name in temperature table
    country_mapping_df_spark = create_country_mapping_df(country_dict)
    
    # 2) group by country
    df = input_df.select(["Country", 'AverageTemperature', 'AverageTemperatureUncertainty'])\
                    .groupBy(col("Country").alias("country")).agg(
                    F.round(F.mean('AverageTemperature'), 2).alias("average_temperature"),
                    F.round(F.mean("AverageTemperatureUncertainty"), 2).alias("average_temperature_uncertainty"))
    
    # 3) right join conutry mapping values with temperature values to get only the desired countries temperature
    df = df.join(country_mapping_df_spark, df.country == country_mapping_df_spark.country_name, how='right') \
            .select(['country_code', 'country_name', 'average_temperature', 'average_temperature_uncertainty'])
    
    
    # write the table to parquet file
    write_to_parquet(df, output_path, table_name)
    
    return df


def create_date_dimension(input_df, output_path, table_name):
    """
        Create Date dimension, model its data, and write the data into parquet files.
        
        Inputs:
            input_df (pyspark dataframe): input dataframe
            output_path (str): the path where to store the output dataframe
            table_name (str): the table name 
            
        Outputs:
            pyspark dataframe: a dataframe representing date dimension
    """
    # create the dimension table
    udf_convert_datetime = udf(lambda x: datetime(1960, 1, 1) + timedelta(days=int(x)), DateType())

    df = input_df.select(F.explode(F.array("arrdate", "depdate")).alias("date_id"))\
                .dropna()\
                .dropDuplicates(["date_id"]) \
                .withColumn("date", udf_convert_datetime("date_id")) \
                .withColumn('year', year('date')) \
                .withColumn('month', month('date')) \
                .withColumn('day', dayofmonth('date')) \
                .withColumn('weekday', dayofweek('date'))
                
    
    # write the table to parquet file
    write_to_parquet(df, output_path, table_name)
    
    return df


def create_immigration_fact(input_df, output_path, table_name):
    """
        Create Immigration fact table, model its data, and write the data into parquet files.
        
        Inputs:
            input_df (pyspark dataframe): input dataframe
            output_path (str): the path where to store the output dataframe
            table_name (str): the table name 
            
        Outputs:
            pyspark dataframe: a dataframe representing migrant dimension
    """
    ## create the fact table
    spark = SparkSession.builder.\
        config("spark.jars.repositories", "https://repos.spark-packages.org/").\
        config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").\
        enableHiveSupport().getOrCreate()
    
    # read dimension tables that need to be joined
    dim_migrant = spark.read.parquet("./data-model-tables/migrant")
    dim_status = spark.read.parquet("./data-model-tables/status")
    dim_visa = spark.read.parquet("./data-model-tables/visa")

    # join tables that don't have similar key with the fact table to immigration to get their keys
    df = input_df \
                .join(dim_visa, (input_df.i94visa == dim_visa.i94visa) & (input_df.visatype == dim_visa.visatype) & (input_df.visapost == dim_visa.visapost)) \
                .join(dim_status, (input_df.entdepa == dim_status.arrival_flag) & (input_df.entdepd == dim_status.departure_flag) & (input_df.matflag == dim_status.match_flag)) \
                .join(dim_migrant, (input_df.biryear == dim_migrant.birth_year) & (input_df.gender == dim_migrant.gender)) \
                .select(["cicid", input_df.arrdate.alias('date_id_arrdate'), input_df.depdate.alias('date_id_depdate'),  
                         input_df.i94cit.alias('country_code_i94cit'), input_df.i94res.alias('country_code_i94res'), input_df.i94port.alias('iata_code_i94port'), 
                         "i94mode", input_df.i94addr.alias('state_code_i94addr'), "visa_id", "status_flag_id", "migrant_id", 
                         "airline", "admnum", "fltno"])
    
    # write the table to parquet file
    write_to_parquet(df, output_path, table_name)
    
    return df