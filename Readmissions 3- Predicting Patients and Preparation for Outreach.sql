--Readmissions 3: Predicting Patients and Preparation for Outreach
--In this vignette we will score our new patients for readmission, create and apply data governance policies to limit the data viewable by our outreach coordinator, create a compute cluster that can scale out to meet the concurrency needs of reporting, and prepare data for visualization in Tableau

--Set context for this vignette
use role datasci;
use schema analytics.readmit;
use warehouse datasci_wh;

--See our model in the internal stage   

--Score new patients with the prediction function of our classifier model
alter warehouse datasci_wh 
    set warehouse_size=xlarge;
    
create or replace table new_patient_predictions_SS_1114 as
    select *, 
        readmissions_classifier!predict(patient_age,bmi,prior_ip_admits,length_of_stay_norm,chronic_conditions_number, sbux_count_imp, diagnosis_ohe_ami, diagnosis_ohe_cabg, diagnosis_ohe_copd, diagnosis_ohe_hf,diagnosis_ohe_hipknee,diagnosis_ohe_pn,patient_gender_ohe_f, patient_gender_ohe_m, marital_status_ohe_n, marital_status_ohe_y, high_na_at_discharge_ohe_y, high_na_at_discharge_ohe_n 
) as predicted_readmission               
    from new_patients_transformed_pipeline;

alter warehouse datasci_wh 
    set warehouse_size=small;
    

select *, predicted_readmission:PREDICTED_READMIT_FLAG as predicted_readmit_flag 
    from new_patient_predictions_SS_1114
    limit 10;
    
--What proportion of our new patient have been predicted to be readmitted? 
select distinct predicted_readmission:PREDICTED_READMIT_FLAG as predicted_readmit_flag, count(*) as count
    from new_patient_predictions_SS_1114 
    group by predicted_readmit_flag;

-- Create view for analytics
create or replace view new_patients_predictions_analytics_view as
    select *, predicted_readmission:PREDICTED_READMIT_FLAG::number as predicted_readmit_flag
     from new_patient_predictions_SS_1114;


    
--Create limited, outreach coordinator role for viewing predictions and contacting those patients
--Grant our outreach coordinator the appropriate permissions
use role accountadmin;    
create or replace role outreach_role;
grant role outreach_role to user [your user here];

use role datasci;
use schema analytics.readmit;
grant usage on database analytics to role outreach_role;
grant usage on schema readmit to role outreach_role;
grant select on table new_patient_predictions_SS_1114 to role outreach_role;
grant select on view new_patients_predictions_analytics_view to role outreach_role;
grant create streamlit on schema analytics.readmit to role outreach_role;
grant create stage on schema analytics.readmit to role outreach_role;

--Create an isolated multi-cluster warehouse for the presentation layer
use role sysadmin;
create or replace warehouse pres_wh
    warehouse_size=xsmall
    min_cluster_count=1
    max_cluster_count=4
    scaling_policy=standard
    auto_suspend=600
    auto_resume=true;
grant usage on warehouse pres_wh to role datasci;
grant usage on warehouse pres_wh to role outreach_role;

use role datasci;

--Create simple masking policies for strings, float numerics, and rounding age
create schema analytics.security;

create or replace masking policy analytics.security.mask_string_simple as
  (val string) returns string ->
  case
    when current_role() in ('DATASCI', 'SYSADMIN', 'ACCOUNTADMIN') then val
      else '**masked**'
    end;
    
create or replace masking policy analytics.security.age_mask_simple as
  (val integer) returns integer ->
  case
    when current_role() in ('DATASCI', 'SYSADMIN', 'ACCOUNTADMIN') then val
      else concat(substr(val, 0, 1), 0)
    end;

create or replace masking policy analytics.security.mask_float_simple as
  (val float) returns float ->
  case
    when current_role() in ('DATASCI', 'SYSADMIN', 'ACCOUNTADMIN') then val
      else 999.999
    end;
    
--Create conditional masking policies based on contact preference
--We will only show the contact method info for the column specified in the contact_preference column
--Only show phone number when patient specified contact via phone or text

create or replace masking policy analytics.security.phone_mask as
    (val string, contact string) returns string ->
    case
        when current_role() in ('OUTREACH_ROLE') and contact in ('phone', 'text') then val
        when current_role() in ('DATASCI', 'SYSADMIN', 'ACCOUNTADMIN') then val
        else 'Phone Masked'
    end;

--Only show email when patient specified contact via email
create or replace masking policy analytics.security.email_mask as 
    (val string, contact string) returns string->
    case
        when current_role() in ('OUTREACH_ROLE') and contact in ('email') then val
        when current_role() in ('DATASCI', 'SYSADMIN', 'ACCOUNTADMIN') then val
        else 'Email Masked'
    end;

--Only show home address when patient specified contact via home visit
create or replace masking policy analytics.security.address_mask as 
    (val string, contact string) returns string->
    case
        when current_role() in ('OUTREACH_ROLE') and contact in ('home visit') then val
        when current_role() in ('DATASCI', 'SYSADMIN', 'ACCOUNTADMIN') then val
        else 'Address Masked'
    end;


--Apply masking policies to our table of predictions  
use database analytics;
alter view readmit.new_patients_predictions_analytics_view modify
    column patient_age set masking policy security.age_mask_simple, 
    column marital_status set masking policy security.mask_string_simple, 
    column bmi set masking policy security.mask_float_simple,
    column total_charges set masking policy security.mask_float_simple,
    column phone set masking policy security.phone_mask using (phone, contact_preference),
    column email set masking policy security.email_mask using (email, contact_preference),
    column address set masking policy security.address_mask using (address, contact_preference); 
    

--Create and apply row access policy so outreach role can only see at-risk individuals
--We don't need out outreach coordinator to reach patients that aren't at risk
create or replace row access policy security.readmissions_RAP as (predicted_readmit_flag number) returns boolean ->
    case
        when current_role() = 'OUTREACH_ROLE' and predicted_readmit_flag=1 then true
        when current_role() in ('OUTREACH_ROLE') then false
        else true
    end;
    
alter view readmit.new_patients_predictions_analytics_view 
    add row access policy security.readmissions_RAP on (predicted_readmit_flag);

--View table with our data scientist role
use role datasci;
use schema analytics.readmit;

select * 
    from new_patients_predictions_analytics_view
    limit 10;

--We can see all patients in the table (37,838)
select count(*) 
    from new_patients_predictions_analytics_view;

--All of the columns we masked are viewable in the clear
select NPI, predicted_readmit_flag, contact_preference, email, phone, address, bmi, patient_age, marital_status, total_charges
    from new_patients_predictions_analytics_view
    limit 15;

--Chart predicted readmission by diagnosis
select diagnosis, predicted_readmit_flag, count(*) as count
    from new_patients_predictions_analytics_view
    group by 1, 2;

--Switch to our limited role
use role outreach_role;
use warehouse pres_wh;
use schema analytics.readmit;

--Our outreach coordinator can only see the patients predicted as readmitted (7,950)
select count(*) 
    from new_patients_predictions_analytics_view;

--Column masks are applied 
select NPI, predicted_readmit_flag, contact_preference, email, phone, address, bmi, patient_age, marital_status, total_charges
    from new_patients_predictions_analytics_view
    limit 15;

--Chart predicted readmission by diagnosis
select diagnosis, predicted_readmit_flag, count(*) as count
    from new_patients_predictions_analytics_view
    group by 1, 2;

