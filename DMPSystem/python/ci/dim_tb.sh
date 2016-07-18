#!/bin/bash
hivebase="dmp_test"   
hiveloc="/user/hive/warehouse/dmptest_user_dir/dmp_test"
hive -e "
use ${hivebase};
CREATE external TABLE IF NOT EXISTS dim_ci_rule_app(
  id string
  ,host string
  ,url_contains string
  ,url_regex string
  ,ua_contains string
  ,ua_regex string
  ,app_id string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '\t' 
STORED AS textfile
LOCATION  '${hiveloc}/dim_ci_rule_app';
  
CREATE external TABLE IF NOT EXISTS dim_ci_rule_site(
  id string
  ,host string
  ,site_id string)
  ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '\t' 
STORED AS textfile
LOCATION '${hiveloc}/dim_ci_rule_site';
  
CREATE external TABLE IF NOT EXISTS dim_ci_rule_loc(
  id string
  ,host string
  ,lng_key string
  ,lng_regex string
  ,lat_regex string
  ,prefix string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '\t' 
STORED AS textfile
LOCATION '${hiveloc}/dim_ci_rule_loc';
     
CREATE external TABLE IF NOT EXISTS dim_ci_rule_user_flag(
  id string
  ,host string
  ,flag_type string
  ,url_key string
  ,url_regex string
  ,cookie_key string
  ,cookie_regex string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '\t' 
STORED AS textfile
LOCATION '${hiveloc}/dim_ci_rule_user_flag';
  
CREATE external TABLE IF NOT EXISTS dim_ci_rule_blacklist(
  id string 
  ,black_type string
  ,black_key string
  )
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '\t' 
STORED AS textfile
LOCATION '${hiveloc}/dim_ci_rule_blacklist';
CREATE external TABLE IF NOT EXISTS dim_ci_rule_cont_action(
id string
,host string
,url_contains string
,url_key string
,url_regex string
,ref_contains string
,ref_key string
,ref_regex string
,cont_id string
,action_id string
,value_type_id string
,prefix string
)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '\t' 
STORED AS textfile
LOCATION '${hiveloc}/dim_ci_rule_cont_action';
"
