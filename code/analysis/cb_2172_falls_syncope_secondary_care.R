#connect R to connected bradford SQL server management studio 
library(readr)
library(DBI)
library(odbc)
library(dplyr)

sql_conn_omop <- dbConnect(odbc::odbc(),
                           driver = "SQL Server",
                           server = "bhts-connectyo3",
                           database = "CB_2172",
                           Trusted_Connection = "True")

# this lists all the tables in the database
query <- "
SELECT TABLE_SCHEMA, TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE'"

#get syncope from secondary care at airedale,calderdale,bradford

syncope_query <-
"
with ed_ch_cohort_2015 as (
  select
  person_id,
  [tbl_SUS_Airedale_ECDS_20150401_to_20220710_mrg_start_date] as episode_start,
  [tbl_SUS_Airedale_ECDS_20150401_to_20220710_mrg_end_date] as episode_end,
  1 as episode_sequence,
  SUBSTRING(diag_group, 1, CHARINDEX('^', diag_group)) AS diagnosis_1,
  NULL AS diagnosis_2,
  NULL AS diagnosis_3
  -- NULL AS diagnosis_4
  from [bhts-connectyor].[src_StagingDatabase_SUS_Airedale].[dbo].[tbl_SUS_Airedale_ECDS_20150401_to_20220710_mrg]
  where person_id in(select person_id from [bhts-connectyo3].[CB_2172].[dbo].[CB_2172.care_home_cohort_v1] )),

episode_ch_cohort_2015 as (
  select
  person_id,
  tbl_SUS_Airedale_APC_20150401_to_20190131_start_date as episode_start,
  tbl_SUS_Airedale_APC_20150401_to_20190131_end_date as episode_end,
  cast(ep_no as bigint) as episode_sequence,
  rtrim(diag1,'X') as diagnosis_1,
  rtrim(diag2,'X') as diagnosis_2,
  rtrim(diag3,'X') as diagnosis_3
  --rtrim(diag4,'X') as diagnosis_4
  from [bhts-connectyor].[src_StagingDatabase_SUS_Airedale].[dbo].[tbl_SUS_Airedale_APC_20150401_to_20190131]
  where person_id in(select person_id from [bhts-connectyo3].[CB_2172].[dbo].[CB_2172.care_home_cohort_v1])),

episode_ch_cohort_2019 as (
  select
  person_id,
  tbl_SUS_Airedale_APC_20190201_to_20200630_start_date as episode_start,
  tbl_SUS_Airedale_APC_20190201_to_20200630_end_date as episode_end,
  cast(ep_no as bigint) as episode_sequence,
  rtrim(diag1,'X') as diagnosis_1,
  rtrim(diag2,'X') as diagnosis_2,
  rtrim(diag3,'X') as diagnosis_3
  -- rtrim(diag4,'X') as diagnosis_4
  from [bhts-connectyor].[src_StagingDatabase_SUS_Airedale].[dbo].[tbl_SUS_Airedale_APC_20190201_to_20200630]
  where person_id in(select person_id from [bhts-connectyo3].[CB_2172].[dbo].[CB_2172.care_home_cohort_v1] )
),

episode_ch_cohort_2020 as (
  select
  person_id,
  tbl_SUS_Airedale_APC_20200701_to_20220710_mrg_start_date as episode_start,
  tbl_SUS_Airedale_APC_20200701_to_20220710_mrg_end_date as episode_end,
  cast(ep_no as bigint) as episode_sequence,
  rtrim(diag1,'X') as diagnosis_1,
  rtrim(diag2,'X') as diagnosis_2,
  rtrim(diag3,'X') as diagnosis_3
  -- rtrim(diag4,'X') as diagnosis_4
  from [bhts-connectyor].[src_StagingDatabase_SUS_Airedale].[dbo].[tbl_SUS_Airedale_APC_20200701_to_20220710_mrg]
  where person_id in(select person_id from [bhts-connectyo3].[CB_2172].[dbo].[CB_2172.care_home_cohort_v1] )
),
-- for some reason the airedale hospital records are split into pre and post 2019 so i had to combine the two tables toether to ensure no episodes were missed

aire_episode_ch_cohort_combined as (
  select 
  *
    from ed_ch_cohort_2015
  
  union all 
  
  select
  *
    from episode_ch_cohort_2015
  
  union all
  
  select
  *
    from episode_ch_cohort_2019
  
  union all
  
  select
  *
    from episode_ch_cohort_2020
),


-- joins care home entry date from the master cohort table
aire_episode_entrydate_join as (
  select
  e_c.*,
  CONVERT(datetime,[episodestartdate],103) as first_episodestartdate
  from aire_episode_ch_cohort_combined as e_c
  left join [bhts-connectyo3].[CB_2172].[dbo].[CB_2172.care_home_cohort_v1]  a
  on e_c.person_id = a.person_id),
-- filters out episodes which began after the admission date to a care home
aire_episode_ch_filtered as (
  select
  e_e_j.*
    from aire_episode_entrydate_join e_e_j
  where (episode_start > first_episodestartdate) AND (DATEDIFF(day,first_episodestartdate,episode_start) < 366)
),
-- filter for the first episode in a spell as to avoid inpatient syncopes being inadvertently captured.
aire_inpatient_episode_ch_final as (
  select
  person_id,
  first_episodestartdate,
  substring(diagnosis_1,1,3) as diagnosis_1_substr,
  substring(diagnosis_2,1,3) as diagnosis_2_substr,
  substring(diagnosis_3,1,3) as diagnosis_3_substr,
  -- substring(diagnosis_4,1,3) as diagnosis_4_substr,
  episode_start,
  episode_end  
  from aire_episode_ch_filtered
  where episode_sequence = 1),
-- up to this point reprents the code to identify and clean all seocndary inpatient episodes for indiivudals in the connected bradford cohort in the 12 months after admission,the next line of code will identify admission related to syncopes, will then union this with a table for syncopes in AE

aire_inpatient_episode_syncope_codes as (
  select
  *,
  -- this line ranks episodes according to the date they occurred for each person, this way we can identify the most recent hospital attendance following admission
  row_number() over (partition by person_id order by episode_start) as admission_seq
  from aire_inpatient_episode_ch_final
  where diagnosis_1_substr in(select [Code] from [CB_2172].[dbo].[syncope_codes]) OR 
  diagnosis_2_substr in(select [Code] from [CB_2172].[dbo].[syncope_codes]) OR 
  diagnosis_3_substr in(select [Code] from [CB_2172].[dbo].[syncope_codes])
),
-- this then filters for the first hopsital spell with a syncope coded in any of the diagnosis codes. it is theoretically possible that a patient had an inpatient syncope which was coded in the co-moribidities rather than the primary reason for presntation, but it is assumed the margin of error is relatively small.
syncope_aire as (
  select
  *
    from aire_inpatient_episode_syncope_codes
  where admission_seq = 1),
---------------------BRADFORD syncopeS-------------------------------------------------------------------------------------------------------------------
  bradford_episode_ch_cohort_ae as (
    select
    cast([person_id] as bigint) as person_id,
    [tbl_ae_start_date],
    [tbl_ae_end_date],
    rtrim([diagnosis_1],'X') as diagnosis_1,
    rtrim([diagnosis_2],'X') as diagnosis_2,
    rtrim([diagnosis_3],'X') as diagnosis_3
    from [bhts-connectyor].[src_StagingDatabase_Warehouse].[dbo].[tbl_ae]
    where cast([person_id] as bigint) in(select cast(person_id as bigint) from [bhts-connectyo3].[CB_2172].[dbo].[CB_2172.care_home_cohort_v1])),

--joins care home entry date from the master cohort table
bradford_episode_entrydate_join_ae as (
  select
  e_c.*,
  convert(datetime,a.date_of_admission,103) first_episodestartdate
  from bradford_episode_ch_cohort_ae as e_c
  left join [bhts-connectyo3].[CB_2172].[dbo].[CB_2172.care_home_cohort_v1] a
  on e_c.person_id  = cast(a.person_id as bigint) ),
-- filters out episodes which began after the admission date to a care home
bradford_episode_ch_filtered_ae as (
  select
  e_e_j.*
    from bradford_episode_entrydate_join_ae e_e_j
  where (tbl_ae_start_date > first_episodestartdate) AND (DATEDIFF(day, first_episodestartdate,tbl_ae_start_date) < 366 )
),
-- filter for the first episode in a spell and extract the first three characters from each icd10 diagnosis code (not interested in the fourth character code for syncopes code)
bradford_episode_ch_final_ae as (
  select
  person_id,
  first_episodestartdate,
  diagnosis_1,
  diagnosis_2,
  diagnosis_3,
  tbl_ae_start_date,
  tbl_ae_end_date,  
  substring(diagnosis_1,1,3) as diagnosis_1_substr,
  substring(diagnosis_2,1,3) as diagnosis_2_substr,
  substring(diagnosis_3,1,3) as diagnosis_3_substr
  from bradford_episode_ch_filtered_ae),
-- up to this point reprents the code to identify and clean all seocndary inpatient episodes for indiivudals in the connected bradford cohort in the 12 months after admission,the next line of code will identify admission related to syncopes, will then union this with a table for syncopes in AE. admissions related to syncopes was defined as any admission with a SNOMED OR ICD10 code available.

bradford_syncope_ch_ae as (
  select
  *,
  row_number() over (partition by person_id order by tbl_ae_start_date) as admission_seq
  from bradford_episode_ch_final_ae
  where bradford_episode_ch_final_ae.diagnosis_1_substr COLLATE Latin1_General_CS_AS in(select Code from  [CB_2172].[dbo].[syncope_codes]) OR
  bradford_episode_ch_final_ae.diagnosis_2_substr COLLATE Latin1_General_CS_AS in(select Code from  [CB_2172].[dbo].[syncope_codes]) OR
  diagnosis_3_substr COLLATE Latin1_General_CS_AS in(select Code from  [CB_2172].[dbo].[syncope_codes]) OR
  diagnosis_1_substr COLLATE Latin1_General_CS_AS in(select cast(code as char) from [CB_2172].[dbo].[syncope_codes] ) or 
  diagnosis_2_substr COLLATE Latin1_General_CS_AS in(select cast(code as char) from [CB_2172].[dbo].[syncope_codes] ) or 
  diagnosis_3_substr COLLATE Latin1_General_CS_AS in(select cast(code as char) from [CB_2172].[dbo].[syncope_codes] )
),
-- this then filters for the first hopsital spell with a syncope coded in any of the diagnosis codes. it is theoretically possible that a patient had an inpatient syncope which was coded in the co-moribdities rather than the primary reason for presntation, but it is assumed the margin of error is relatively small.

bradford_syncope_ae_final as (
  select
  *
    from bradford_syncope_ch_ae
  where admission_seq = 1
),

-- then we take inpatient icd10 diagnosis for BRI
bradford_episode_ch_cohort as (
  
  select
  cast(person_id as varchar) AS person_id ,
  tbl_episode_start_date,
  tbl_episode_end_date,
  episode_number,
  spell_number,
  cast(episode_sequence as bigint) as episode_sequence,
  current_episode,
  last_episode_in_spell,
  diagnoses,
  rtrim(diagnosis_1,'X') as diagnosis_1,
  rtrim(diagnosis_2,'X') as diagnosis_2,
  rtrim(diagnosis_3,'X') as diagnosis_3
  from [bhts-connectyor].[src_StagingDatabase_Warehouse].[dbo].[tbl_episode]
  where person_id in(select cast(person_id as bigint) from [bhts-connectyo3].[CB_2172].[dbo].[CB_2172.care_home_cohort_v1]  )),

--joins care home entry date from the master cohort table
bradford_episode_entrydate_join as (
  select
  e_c.*,
  convert(datetime,a.date_of_admission, 103) as  first_episodestartdate
  from bradford_episode_ch_cohort as e_c
  left join [bhts-connectyo3].[CB_2172].[dbo].[CB_2172.care_home_cohort_v1]  a
  on e_c.person_id  = cast(a.person_id as bigint) ),
-- filters out episodes which began after the admission date to a care home
bradford_episode_ch_filtered as (
  select
  e_e_j.*
    from bradford_episode_entrydate_join e_e_j
  where (tbl_episode_start_date > first_episodestartdate) AND (DATEDIFF(day, first_episodestartdate,tbl_episode_start_date) < 366 )
),
-- filter for the first episode in a spell and extract the first three characters from each icd10 diagnosis code (not interested in the fourth character code for syncopes code)
bradford_episode_ch_final as (
  select
  cast(person_id as bigint) as person_id,
  first_episodestartdate,
  diagnosis_1,
  diagnosis_2,
  diagnosis_3,
  tbl_episode_start_date,
  tbl_episode_end_date,  
  substring(diagnosis_1,1,3) as diagnosis_1_substr,
  substring(diagnosis_2,1,3) as diagnosis_2_substr,
  substring(diagnosis_3,1,3) as diagnosis_3_substr
  from bradford_episode_ch_filtered
  where episode_sequence = 1
),
-- up to this point reprents the code to identify and clean all seocndary inpatient episodes for indiivudals in the connected bradford cohort in the 12 months after admission,the next line of code will identify admission related to syncopes, will then union this with a table for syncopes in AE. admissions related to syncopes was defined as any admission with a SNOMED OR ICD10 code available.

bradford_syncope_ch as (
  select
  *,
  row_number() over (partition by person_id order by tbl_episode_start_date) as admission_seq
  from bradford_episode_ch_final
  where bradford_episode_ch_final.diagnosis_1_substr COLLATE Latin1_General_CS_AS in(select Code from  [CB_2172].[dbo].[syncope_codes]) 
  OR bradford_episode_ch_final.diagnosis_2_substr COLLATE Latin1_General_CS_AS in(select Code from  [CB_2172].[dbo].[syncope_codes]) 
  OR bradford_episode_ch_final.diagnosis_3_substr COLLATE Latin1_General_CS_AS in(select Code from  [CB_2172].[dbo].[syncope_codes]) 
  or bradford_episode_ch_final.diagnosis_1_substr COLLATE Latin1_General_CS_AS in(select cast(code as char) from [CB_2172].[dbo].[syncope_codes] ) 
  or diagnosis_2_substr COLLATE Latin1_General_CS_AS in(select cast(code as char) from [CB_2172].[dbo].[syncope_codes] ) 
  or bradford_episode_ch_final.diagnosis_3_substr COLLATE Latin1_General_CS_AS in(select cast(code as char) from [CB_2172].[dbo].[syncope_codes] )
),
-- this then filters for the first hopsital spell with a syncope coded in any of the diagnosis codes. it is theoretically possible that a patient had an inpatient syncope which was coded in the co-moribdities rather than the primary reason for presntation, but it is assumed the margin of error is relatively small.

bradford_syncope_ch_inpatient as (
  select
  *
    from bradford_syncope_ch
  where admission_seq = 1),

syncope_bradford as (
  select
  cast(person_id as bigint) as person_id,
  first_episodestartdate,
  tbl_episode_start_date
  from bradford_syncope_ch_inpatient
  
  UNION
  
  select
  cast(person_id as bigint) as person_id,
  first_episodestartdate,
  tbl_ae_start_date
  from bradford_syncope_ae_final),

-- then we union distint inpatient episodes and ED episodes together before adding to airedale
----------------CALDERDALE-------------------------------
calderdale_apc as (
select 
person_id, 
tbl_SUS_Calderdale_APC_CM_20170618_To_20220731_start_date as admission_start_date,
tbl_SUS_Calderdale_APC_CM_20170618_To_20220731_end_date as admission_end_date,
1 as episode_sequence, 
diagnosis_primary_icd as diagnosis_1, 
diagnosis_1st_secondary_icd as diagnosis_2,
diagnosis_2nd_secondary_icd as diagnosis_3
from [bhts-connectyor].[src_StagingDatabase_SUS_Calderdale].[dbo].[tbl_SUS_Calderdale_APC_CM_20170618_To_20220731]),

calderdale_episode_entrydate_join as (
  select
  e_c.*,
  CONVERT(datetime,a.episodestartdate,103) as first_episodestartdate
  from calderdale_apc as e_c
  left join [bhts-connectyo3].[CB_2172].[dbo].[CB_2172.care_home_cohort_v1]  a
  on e_c.person_id = a.person_id),
-- filters out episodes which began after the admission date to a care home
calderdale_episode_ch_filtered as (
  select
  e_e_j.*
    from calderdale_episode_entrydate_join e_e_j
  where (admission_start_date > first_episodestartdate) AND (DATEDIFF(day,first_episodestartdate,admission_start_date) < 366)
),
-- filter for the first episode in a spell as to avoid inpatient syncopes being inadvertently captured.
calderdale_inpatient_episode_ch_final as (
  select
  person_id,
  first_episodestartdate,
  substring(diagnosis_1,1,3) as diagnosis_1_substr,
  substring(diagnosis_2,1,3) as diagnosis_2_substr,
  substring(diagnosis_3,1,3) as diagnosis_3_substr,
  -- substring(diagnosis_4,1,3) as diagnosis_4_substr,
admission_start_date as episode_start,
  admission_end_date as episode_end  
  from calderdale_episode_ch_filtered
  where episode_sequence = 1),
-- up to this point reprents the code to identify and clean all seocndary inpatient episodes for indiivudals in the connected bradford cohort in the 12 months after admission,the next line of code will identify admission related to syncopes, will then union this with a table for syncopes in AE

calderdale_inpatient_episode_syncope_codes as (
  select
  *,
  -- this line ranks episodes according to the date they occurred for each person, this way we can identify the most recent hospital attendance following admission
  row_number() over (partition by person_id order by episode_start) as admission_seq
  from calderdale_inpatient_episode_ch_final
  where diagnosis_1_substr in(select [Code] from [CB_2172].[dbo].[syncope_codes]) OR 
  diagnosis_2_substr in(select [Code] from [CB_2172].[dbo].[syncope_codes]) OR 
  diagnosis_3_substr in(select [Code] from [CB_2172].[dbo].[syncope_codes])
),
-- this then filters for the first hopsital spell with a syncope coded in any of the diagnosis codes. it is theoretically possible that a patient had an inpatient syncope which was coded in the co-moribidities rather than the primary reason for presntation, but it is assumed the margin of error is relatively small.
calderdale_syncope as (
  select
  *
    from calderdale_inpatient_episode_syncope_codes
  where admission_seq = 1),

--union bradford airedale and calderdale 


syncope_union as (
  select
  person_id,
  first_episodestartdate,
  tbl_episode_start_date
  from syncope_bradford
  
  
  union all
  
  select
  person_id,
  syncope_aire.first_episodestartdate,
  syncope_aire.episode_start
  from
  syncope_aire
  
  union all 

  select
  person_id,
  first_episodestartdate,
  episode_start
  from calderdale_syncope),

sequenced as (
  select
  *,
  row_number() over (partition by person_id order by tbl_episode_start_date) admission_sequence
  from syncope_union)

select
*,
'syncope' as event_type
  from sequenced
where admission_sequence = 1
;
"
syncope_table <- dbGetQuery(sql_conn_omop,syncope_query)

#get injjurious falls 

falls_query <- "
select
[person_id],   'falls' as event_type
      ,[first_episodestartdate]
      ,[tbl_episode_start_date]
   
from 
[cb_2172].[dbo].[cb_2172_injurious_falls_v3_20250130114221]
where person_id in (select person_id from [bhts-connectyo3].[CB_2172].[dbo].[CB_2172.care_home_cohort_v1]);"

injurious_falls <- dbGetQuery(sql_conn_omop,falls_query)

care_home_query <- "
select
*
from [bhts-connectyo3].[CB_2172].[dbo].[CB_2172.care_home_cohort_v1]"

care_home_table <- dbGetQuery(sql_conn_omop,care_home_query)
#bind together falls and syncope to calculate incidence density in falls and syncope 
injurious_falls$first_episodestartdate <-  as.POSIXct(injurious_falls$first_episodestartdate,format = "%Y-%m-%d %H:%M:%M")
injurious_falls$tbl_episode_start_date <-  as.POSIXct(injurious_falls$tbl_episode_start_date,format = "%Y-%m-%d %H:%M:%M")
falls_syncope_care_home <- rbind(injurious_falls,select(syncope_table,person_id,first_episodestartdate,tbl_episode_start_date,event_type))
colnames(falls_syncope_care_home) <- c("person_id","event_type","care_home_admission_date","clinical_event_date")
#exclude events occurring on same day as ch admission
falls_syncope_care_home <- subset(falls_syncope_care_home, clinical_event_date - care_home_admission_date > 24)

falls_syncope_care_home<- falls_syncope_care_home %>% group_by(person_id) %>% arrange(clinical_event_date,.by_group = TRUE) %>% mutate(admission_rank= row_number())
falls_syncope_care_home <- subset(falls_syncope_care_home,admission_rank == 1 )
falls_syncope_care_home$exposure_time <- as.numeric(difftime(falls_syncope_care_home$clinical_event_date,falls_syncope_care_home$care_home_admission_date,units="days"))

care_home_table$date.event <-as.POSIXct(care_home_table$dod,format = "%d/%m/%Y") %>% as_date()
care_home_table <- care_home_table %>% select(-dod)
care_home_table <- care_home_table %>% arrange(person_id, desc(date.event)) %>% mutate(event_id= rank(date.event))

#join falls table and care home cohort maaster table 
ch_fall_merged <- merge(x =care_home_table, y = falls_syncope_care_home[c(1,2,3,4,6)], all.x = TRUE) %>% select(-episodestartdate,-care_home_admission_date,-PostCodeDistrict)
#create exposure time variable to calculate incidence density
ch_fall_merged$admission_date <-as.POSIXct(ch_fall_merged$date_of_admission,format = "%d/%m/%Y") %>% as_date()
ch_fall_merged$clinical_event_date <- as_date(ch_fall_merged$clinical_event_date)
ch_fall_merged$exposure_time <- 0

for(i in 1:nrow(ch_fall_merged)) {
  if(!is.na(ch_fall_merged[i,]$clinical_event_date)) {
    ch_fall_merged[i,]$exposure_time <- ch_fall_merged[i,]$clinical_event_date - ch_fall_merged[i, ]$admission_date
  } else if (is.na(ch_fall_merged[i, ]$clinical_event_date) & !is.na(ch_fall_merged[i, ]$date.event) & 
             ((ch_fall_merged[i, ]$date.event - ch_fall_merged[i, ]$admission_date) < 365)) {
    ch_fall_merged[i,]$exposure_time <- ch_fall_merged[i,]$date.event - ch_fall_merged[i,]$admission_date
  } else {
    ch_fall_merged[i,]$exposure_time <- 365
  }
}

ch_fall_merged$exposure_time <- as.numeric(ch_fall_merged$exposure_time)


#for those with missing death dates, it waa assumed they had 12 months of exposure
#set the exposure time for those alive more 12 months afterwards to 365 days to calculate incidence density in the year after admission

ch_fall_merged <- ch_fall_merged %>% mutate(fall = if_else(!is.na(clinical_event_date),TRUE,FALSE))

#add hypertension diagnosis

hypertension_query <- "
SELECT  [person_id]
      ,[hypertension]
      ,[first_episodestartdate]
  FROM [CB_2172].[dbo].[cb_2172_hypertension_combined_20250130114221]"

ch_htn <- dbGetQuery(sql_conn_omop,hypertension_query)

ch_fall_htn_merged <- merge(x=ch_fall_merged,y=ch_htn,by.x='person_id',by.y='person_id',all.x= TRUE) %>% select(-first_episodestartdate)

#then we caluclate the inciddence rate by dividing the total number of falls in the cohort by the exposure time in person years

incidnece_density_falls <- (sum(!is.na(ch_fall_htn_merged$event_type))/(ch_fall_htn_merged$exposure_time %>% sum()/365))*100
exposure_time_years <- ch_fall_merged$exposure_time %>% sum()/365

pois.exact(sum(!is.na(ch_fall_htn_merged$event_type)), pt= exposure_time_years,conf.level=0.95)

######## sensitivity analysis COVID 19 #######################

#determine if falls/syncope differs between those exposed durign COVID-19 perio and those not 

#split cohort those admitted before march 2019 and after march 2019

fall_syncope_pre_march_19 <- ch_fall_htn_merged %>% filter(admission_date < '2019-03-20')

fall_syncope_post_march_19 <- ch_fall_htn_merged %>% filter(admission_date > '2019-03-20')

#incidence density pre march 2019 

incidence_density_falls_pre_march_19 <- (sum(!is.na(fall_syncope_pre_march_19$event_type))/(fall_syncope_pre_march_19$exposure_time %>% sum()/365))*100

exposure_time_years_pre_march_19 <- fall_syncope_pre_march_19$exposure_time %>% sum()/365

event_count_pre_march_19 <- sum(!is.na(fall_syncope_pre_march_19$event_type))

pois.exact(sum(!is.na(fall_syncope_pre_march_19$event_type)), pt= exposure_time_years,conf.level=0.95)

#incidence density post march 2019 

incidence_density_falls_post_march_19 <- (sum(!is.na(fall_syncope_post_march_19$event_type))/(fall_syncope_post_march_19$exposure_time %>% sum()/365))*100

exposure_time_years_post_march_19 <- fall_syncope_post_march_19$exposure_time %>% sum()/365

event_count_post_march_19 <- sum(!is.na(fall_syncope_post_march_19$event_type))

pois.exact(sum(!is.na(fall_syncope_post_march_19$event_type)), pt= exposure_time_years,conf.level=0.95)

#poisson test (Exact) to determine difference between pre and post march

poisson.test(c(event_count_pre_march_19,event_count_post_march_19),c(exposure_time_years_pre_march_19,exposure_time_years_post_march_19))

