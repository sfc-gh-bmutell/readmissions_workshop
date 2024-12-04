use role accountadmin;
create role datasci;
grant create database on account to role datasci;
grant create warehouse on account to role datasci;
grant import share on account to role datasci;

grant role datasci to user [your user name];
use role datasci;


/*Navigate to home page*/
-- Create warehouse LOAD_WH
-- Create database ANALYTICS
-- Create schema READMIT
-- Create table NEW_PATIENTS from CSV
-- Create stage LOAD_STAGE
-- Upload READMISSIONS_RAW csv
