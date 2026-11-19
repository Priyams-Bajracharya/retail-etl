-- Run first. Safe to run more than once.
CREATE DATABASE IF NOT EXISTS retail_etl;
CREATE SCHEMA IF NOT EXISTS retail_etl.bronze;
CREATE SCHEMA IF NOT EXISTS retail_etl.silver;
CREATE SCHEMA IF NOT EXISTS retail_etl.gold;