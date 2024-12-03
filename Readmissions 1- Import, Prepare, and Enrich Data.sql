-- Set context for this worksheet
use role datasci;
use schema analytics.readmit;
use warehouse load_wh;


--Inspect our readmission files stored in internal data lake storage
ls @load_stage;


--Define file format 
create or replace file format my_csv_format
    type = csv
    record_delimiter='\n'
    field_delimiter=','
    skip_header = 1
    null_if = ('NULL', 'null','')
    empty_field_as_null = true
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    date_format='DD-MON-YY'
    error_on_column_count_mismatch=false
   ;

--Define tables for our data   
create or replace table readmissions_raw
    (city_lat float,
    city_long float, 
    hospital_lat float,
    hospital_long float,
    hospital_name varchar,
    hospital_state varchar, 
    diagnosis string, 
    patient_number string, 
    dv_readmit_flag int, 
    admit_date date , 
    length_of_stay int,
    prior_ip_admits int, 
    chronic_conditions_number int, 
    patient_age int, 
    order_set_used int, 
    hospital_id string, 
    hospital_address varchar,
    hospital_city varchar,
    hospital_zip_code string,
    hospital_county_name string,
    hospital_region string,
    npi string,
    total_charges float,
    discharge_notes varchar,
    patient_gender string,
    urban_class string,
    marital_status string,
    sdoh_flag string,
    high_na_at_discharge string,
    bmi float,
    city string,
    state string,
    postcode string,
    address varchar,
    patient_id string,
    patient_lon float,
    patient_lat float,
    contact_preference string,
    email varchar,
    phone string);
    
--Copy the files into the tables we just defined                                                                           
copy into readmissions_raw
    from @load_stage/readmissions_raw.csv
    file_format=my_csv_format
    on_error=continue;
        
--Inspect the data
select *
    from readmissions_raw
    limit 100;
    
describe table readmissions_raw;

--What proportion of our patients have been readmitted?
--Create a bar chart with the results    
select distinct dv_readmit_flag, count(*)
    from readmissions_raw
    group by dv_readmit_flag;
--Inspect query profile

-- Re run query and check query profile
select distinct dv_readmit_flag, count(*)
    from readmissions_raw
    group by dv_readmit_flag;

-- drop the table
drop table readmissions_raw;

select distinct dv_readmit_flag, count(*)
    from readmissions_raw
    group by dv_readmit_flag;

undrop table readmissions_raw;

select distinct dv_readmit_flag, count(*)
    from readmissions_raw
    group by dv_readmit_flag;




--Let's enrich our first party data with third party data from the Snowflake Marketplace
--Jump out to the Marketplace and search for data by zip code, since we have patient zip
--Get data from Free POI Data Sample: US Starbucks locations by Safegraph
--Name the database sample_us_starbucks_locations and grant access to datasci role
use role datasci;
select *
    from Sample_US_Starbucks_locations.public.core_poi
    limit 10;

--Since each record is a distinct location, lets aggregate the number of locations by zip code
create or replace table analytics.readmit.starbucks_zip_agg as
    select distinct postal_code, count(*) as sbux_count
    from sample_us_starbucks_locations.public.core_poi 
    group by postal_code;

select * 
    from analytics.readmit.starbucks_zip_agg;
    
--Join this zip code data to our patients tables
alter warehouse load_wh
    set warehouse_size=medium;
    
create or replace table readmissions_enriched as
    select a.*, b.sbux_count
        from readmissions_raw a
        left join starbucks_zip_agg b
        on a.postcode=b.postal_code;

select * 
    from readmissions_enriched
    limit 100;

update readmissions_enriched
set diagnosis = case 
    when diagnosis = 'HIP/KNEE' then 'HIPKNEE'
    else diagnosis
end;    


create or replace table new_patients_enriched as
    select a.*,  b.sbux_count
        from new_patients a
        left join starbucks_zip_agg b
        on a.postcode=b.postal_code;

update new_patients_enriched
set diagnosis = case 
    when diagnosis = 'HIP/KNEE' then 'HIPKNEE'
    else diagnosis
end;  


select *
    from new_patients_enriched
    where sbux_count is not null 
        and diagnosis='HIPKNEE'
    limit 100;

-- Create specific warehouse for data science modeling in the next vignette
create or replace warehouse datasci_wh
    warehouse_size=xlarge
    auto_resume=true
    auto_suspend=60;

--Now that we have our data ready for modeling, let's just over to Snowpark for Python
