-- WRITING A SCRIPT FOR CREATING tAble for  
-- DROP DATABASE IF EXISTS WeatherDB2;

-- DROPPING the user
DROP USER IF EXISTS 'root'@'localhost';

-- CREATE A NEW DATABASE NAME WeatherDB 
CREATE DATABASE WeatherDB2;

-- SELECT THE WeatherDB 
USE WeatherDB2;

CREATE TABLE weather_records(
    record_fid VARCHAR(11),
    record_date date NOT NULL,
    record_maxtemp INT,
    record_mintemp INT,
    record_precipitation INT,
    PRIMARY KEY(record_fid,record_date)
);

CREATE TABLE station_results(
    result_fid VARCHAR(11),
    result_year INT, 
    result_avg_maxtemp VARCHAR(15),
    result_avg_mintemp  VARCHAR(15),
    result_total_precipitation  VARCHAR(15),
    PRIMARY KEY(result_fid,result_year)
);

CREATE USER 'root'@'localhost' IDENTIFIED BY '12345';
GRANT ALL PRIVILEGES ON WeatherDB2.* TO 'root'@'localhost';
