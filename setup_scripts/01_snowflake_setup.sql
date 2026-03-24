-- =============================================================================
-- Snowflake Initial Setup
-- =============================================================================
-- Run this in a Snowflake worksheet before starting the project.
-- Prerequisites: Snowflake account (free trial works)
-- =============================================================================

-- Create project database
CREATE DATABASE IF NOT EXISTS NYC_TAXI_DB;

-- Create compute warehouse (XSmall for cost efficiency)
-- AUTO_SUSPEND: shuts down after 5 min idle to save credits
-- AUTO_RESUME: wakes up automatically on next query
CREATE WAREHOUSE IF NOT EXISTS NYC_TAXI_WH 
  WITH WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 300
  AUTO_RESUME = TRUE;

-- Create schemas for each DBT layer
USE DATABASE NYC_TAXI_DB;
CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS INTERMEDIATE;
CREATE SCHEMA IF NOT EXISTS MARTS;