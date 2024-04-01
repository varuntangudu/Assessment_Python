#!/usr/bin/env python3

# Module to install to work in the code


# Logging Module
import logging
import os
import sys
import datetime

#Importing the modules
import mysql.connector
from mysql.connector import Error

# file reading and handling data
import numpy as np
import pandas as pd


# REST API
from flask import Flask
from flask import jsonify
from flask_swagger import swagger

import warnings
warnings.filterwarnings("ignore")


def getSQLConnection(logger):
    # Connecting with MySQL
    logger.info('\t SQL Connection Initiated')

    try:
        conn = mysql.connector.connect(host='localhost',
                                             database='weatherDB2',
                                             user='root',
                                             password='12345')

        if conn.is_connected():
            print(f"Connected to MySQL Server version: {conn.get_server_info()}")
        logger.info('\t SQL Connnection was successful for weather data.')
        return conn
    except Error as e:
        print("Error while connecting to MySQL", e)
        logger.info("Error while connecting to MySQL" + e)
        sys.exit(-1)

def createdb(conn,logger):

    mycursor = conn.cursor()

    try:
        print('Creating Database: weatherdb2.sql')
        logger.info('Creating Database: weatherdb2.sql')
        mycursor.execute("create database weatherdb2");
    except:
        print('Error: weatherdb2 already exists\n')
        logger.info('Error: weatherdb2 already exists')

    print('Using Database: weatherdb2.sql\n')
    logger.info('Using Database: weatherdb2.sql')
    mycursor.execute("use weatherdb2")

    try:
        print('Creating table weather_records')
        logger.info('Creating create table weather_records')
        mycursor.execute("CREATE TABLE weather_records( record_fid VARCHAR(11), record_date date NOT NULL, record_maxtemp INT, record_mintemp INT, record_precipitation INT, PRIMARY KEY(record_fid,record_date)) ")
    except:
        print('Error: weather_records table already exists\n')
        logger.info('Error: weather_records table already exists')

    try:
        print('Creating table station_results')
        logger.info('Creating table station_results')
        mycursor.execute("CREATE TABLE station_results( result_fid VARCHAR(11), result_year INT, result_avg_maxtemp Decimal(10,2), result_avg_mintemp  Decimal(10,2), result_total_precipitation  Decimal(10,2), PRIMARY KEY(result_fid,result_year))")
    except:
        print('Error: station_results table already exists\n')
        logger.info('Error: station_results table already exists')

    print('Database Created/Tested Successfully')

def populateData(conn,logger):

    mycursor = conn.cursor()

    # ## Problem 2 : Ingestion
    folderName = 'wx_data'
    folderFound = True
    logger.info('Data Ingestion: started in Weather Project')
    if not os.path.exists(folderName):
        logger.info(f'Error: {folderName} folder doesn\'t exists in current working directory.')
        print(f'Error: {folderName} folder doesn\'t exists in current working directory.')
        folderFound = False

    if folderFound:
        logger.info('\t' + str(datetime.datetime.now()) + ' : Starting Ingestion into MySQL from wx_data')
        print('\t' + str(datetime.datetime.now()) + ' : Starting Ingestion into MySQL from wx_data')
        insertQuery = "INSERT INTO weather_records (record_fid, record_date, record_maxtemp, record_mintemp, record_precipitation) VALUES (%s, %s, %s, %s, %s)"

        # to count number of records inserted  into weather_records table
        fileCount = 0

        # Reading file one by one and inserting the data into mySQL
        for filename in os.listdir(folderName):
            fid = filename.split('.')[0]
            with open(os.path.join(folderName,filename),'r') as f:
                for line in f:
                    data = line.strip().split('\t')
                    data = [ temp.lstrip() for temp in data ]
                    query = f"select * from weather_records where record_fid = '{fid}' and record_date = '{data[0]}'"
                    mycursor.execute(query)
                    if len(mycursor.fetchall()) == 0 :
                        val = (fid, data[0], data[1], data[2], data[3])
                        mycursor.execute(insertQuery, val)
                        conn.commit()
                        fileCount += 1
        logger.info(f'\tNumber of records ingested in weather_records table : {fileCount}')
        print(f'\tNumber of records ingested in weather_records table : {fileCount}')
        logger.info('\t' + str(datetime.datetime.now()) + ' : Stopping Ingestion into MySQL from wx_data')
        print('\t' + str(datetime.datetime.now()) + ' : Stopping Ingestion into MySQL from wx_data')
    else:
        print(f'Error : {folderName} not found')
        logger.info(f'\tError: {folderName} not found.')

def performDataAnalysis(conn,logger):

    mycursor = conn.cursor()

    # ## Problem 3 : Data Analysis
    logger.info('Data Analysis Started.')
    # ### => Extract data from MySQL Data
    logger.info('\t' + 'Building dataframe by reading data from MySQL')
    query = 'select * from weather_records'
    df = pd.read_sql(query, con = conn)

    # ### => copy data to create new dataframe and create new column with year from date column
    newdf = df.copy()
    logger.info('\t' + 'Copying into new dataframe.')
    newdf['record_date'] = newdf['record_date'].astype(str)
    temp = newdf['record_date'].str.split("-", n=1, expand=True)
    newdf['year'] = temp[0]
    newdf['year'] = newdf['year'].astype(int)
    logger.info('\t' + "Adding anothor column 'year' into newdf")
    newdf.drop(['record_date'],axis=1,inplace=True)
    logger.info('\t' + 'removing record_date column from newdf')


    # ### Replacing -9999 to np.nan in the dataframe
    newdf.replace({-9999: np.nan},inplace=True)

    insertQuery = "INSERT INTO station_results (result_fid, result_year, result_avg_maxtemp, result_avg_mintemp, result_total_precipitation ) VALUES (%s, %s, %s, %s, %s)"
    logger.info('\t' + 'Inserting result into station_records table.')

    # from dataframe reading each stations code one by one
    for station in newdf.record_fid.unique():

        print('')
        print("*"*100)
        print('\t{:15} {:8} {:13} {:13} {:18}' .format("Station","Year","AvgMaxTemp","AvgMinTemp","Total Accumulated Precipitation"))
        print("*"*100)
        # Generating year from 1985 to 2014
        for year in range(1985, 2015):

            avgMaxTemp = round(newdf[ (newdf.record_fid == station ) & (newdf.year == year)].record_maxtemp.mean(skipna=True),2)
            avgMinTemp = round(newdf[ (newdf.record_fid == station ) & (newdf.year == year)].record_mintemp.mean(skipna=True),2)
            sumPrecipitation = round(newdf[(newdf.record_fid == station ) & (newdf.year == year)].record_precipitation.sum(skipna=True),2)

            print('\t{:15} {:4} {:>11.2f} {:>11.2f} {:>16.2f}' .format(station,year,avgMaxTemp,avgMinTemp,sumPrecipitation))

            query = f"select * from station_results where result_fid = '{station}' and result_year = '{year}'"
            mycursor.execute(query)

            if len(mycursor.fetchall()) == 0 :

                val = [station, year, avgMaxTemp, avgMinTemp, sumPrecipitation]

                # any nan value should be written in SQL as None
                val = [None if type(y) == float and np.isnan(y) else y for y in val]

                mycursor.execute(insertQuery, val)
                conn.commit()


    ### Reading results from station_results table
    # Build dataframe from MySQL data
    logger.info('\t' + 'Reading results from MySQL station_results table')
    query = 'select * from station_results'
    rdf = pd.read_sql(query, con = conn)
    print(f'Number of records in station_results table : {rdf.shape[0]}')
    print(rdf.to_json(orient='records'))

def main():

    # Starting Logging
    logger = logging.getLogger(__name__)
    logging.basicConfig(filename='weather_data.log', level=logging.INFO)
    logger.info('Log Started: Weather Data Modeling and Analysis Project')

    # connecting to SQL
    conn = getSQLConnection(logger)
    mycursor = conn.cursor()

    if sys.argv[1] == "1":
        ## Problem 1 : Data Modeling
        """
        I have built the 'weatherDB' Schema in SQL and executed the script that
        created the schema and then created two tables: the weather_records table
        in which we will save the data and the station_results table to record
        the statistics of the results.
        """
        createdb(conn,logger)

    elif sys.argv[1] == "2":

        try:
            mycursor.execute("use weatherdb2")
        except:
            print("Error: weatherdb2 doesn't exist")
            createdb(conn,logger)
        populateData(conn,logger)

    elif sys.argv[1] == "3":
        query = "use weatherdb2"
        try:
            mycursor.execute(query)
        except:
            print("Error: weatherdb2 doesn't exist")
            createdb(conn,logger)
            populateData(conn,logger)
        performDataAnalysis(conn,logger)

    logger.info('\t SQL Connection is closed now!')
    # Closing MySQL Connection
    conn.close()

if __name__ == "__main__":
    main()

