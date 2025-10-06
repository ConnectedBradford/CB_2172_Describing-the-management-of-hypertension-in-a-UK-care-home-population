library(odbc)
library(DBI)
library(dplyr)
library(ggplot2)
library(tidyverse)
library(lubridate)
library(finalfit)
library(stats)
library(epitools)

sql_conn_omop <- dbConnect(odbc::odbc(),
                           driver = "SQL Server",
                           server = "bhts-connectyor",
                           database = "CB_OMOP_BTHFT",
                           Trusted_Connection = "True")

sql_conn <- dbConnect(odbc::odbc(),
                      driver = "SQL Server",
                      server = "bhts-connectyo3",
                      database = "CB_2172",
                      Trusted_Connection = "True")

#load in injurious falls table 

falls_query <- "-- combined bradford and airedale queries for injurious falls 
-- airedale ED data 
with ed_ch_cohort_2015 as (
select
person_id,
[tbl_SUS_Airedale_ECDS_20150401_to_20220710_mrg_start_date] as episode_start,
[tbl_SUS_Airedale_ECDS_20150401_to_20220710_mrg_end_date] as episode_end,
1 as episode_sequence,
SUBSTRING(diag_group, 1, CHARINDEX('^', diag_group)) AS diagnosis_1,
NULL AS diagnosis_2,
NULL AS diagnosis_3,
NULL AS diagnosis_4
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
rtrim(diag3,'X') as diagnosis_3,
rtrim(diag4,'X') as diagnosis_4
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
rtrim(diag3,'X') as diagnosis_3,
rtrim(diag4,'X') as diagnosis_4
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
rtrim(diag3,'X') as diagnosis_3,
rtrim(diag4,'X') as diagnosis_4
from [bhts-connectyor].[src_StagingDatabase_SUS_Airedale].[dbo].[tbl_SUS_Airedale_APC_20200701_to_20220710_mrg]
where person_id in(select person_id from [bhts-connectyo3].[CB_2172].[dbo].[CB_2172.care_home_cohort_v1] )
),
-- for some reason the airedale hospital records are split into pre and post 2019 so i had to combine the two tables toether to ensure no episodes were missed

episode_ch_cohort_combined as (
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
episode_entrydate_join as (
  select
  e_c.person_id,
  convert(datetime,e_c.episode_start,120) as hopsital_attendance_date,
  CONVERT(datetime,[episodestartdate],103) as ch_admission_date,
  episode_sequence,
  diagnosis_1,
  diagnosis_2,
  diagnosis_3,
  diagnosis_4,
  episode_end
  from episode_ch_cohort_combined as e_c
  left join [bhts-connectyo3].[CB_2172].[dbo].[CB_2172.care_home_cohort_v1]  a
  on e_c.person_id = a.person_id),
-- filters out episodes which began after the admission date to a care home
episode_ch_filtered as (
  select
e_e_j.*
  from episode_entrydate_join e_e_j
  where (hopsital_attendance_date > ch_admission_date) AND (DATEDIFF(day,ch_admission_date,hopsital_attendance_date) < 366)
),
-- filter for the first episode in a spell as to avoid inpatient falls being inadvertently captured.
inpatient_episode_ch_final as (
select
person_id,
ch_admission_date,
substring(diagnosis_1,1,3) as diagnosis_1_substr,
substring(diagnosis_2,1,3) as diagnosis_2_substr,
substring(diagnosis_3,1,3) as diagnosis_3_substr,
substring(diagnosis_4,1,3) as diagnosis_4_substr,
hopsital_attendance_date,
episode_end  
from episode_ch_filtered
where episode_sequence = 1),
-- up to this point reprents the code to identify and clean all seocndary inpatient episodes for indiivudals in the connected bradford cohort in the 12 months after admission,the next line of code will identify admission related to falls, will then union this with a table for falls in AE

inpatient_episode_fall_codes as (
select
*,
-- this line ranks episodes according to the date they occurred for each person, this way we can identify the most recent hospital attendance following admission
row_number() over (partition by person_id order by hopsital_attendance_date) as admission_seq
from inpatient_episode_ch_final
where inpatient_episode_ch_final.diagnosis_1_substr in(select [Code] from [bhts-connectyo3].[CB_2172].[dbo].[falls__fragilityfracture_icd10_20250130114221]) OR 
diagnosis_2_substr in(select [Code] from [bhts-connectyo3].[CB_2172].[dbo].[falls__fragilityfracture_icd10_20250130114221]) OR 
diagnosis_3_substr in(select [Code] from [bhts-connectyo3].[CB_2172].[dbo].[falls__fragilityfracture_icd10_20250130114221]) OR
diagnosis_4_substr in(select [Code] from [bhts-connectyo3].[CB_2172].[dbo].[falls__fragilityfracture_icd10_20250130114221])
),
-- this then filters for the first hopsital spell with a fall coded in any of the diagnosis codes. it is theoretically possible that a patient had an inpatient fall which was coded in the co-moribidities rather than the primary reason for presntation, but it is assumed the margin of error is relatively small.
fall_aire as (
select
*
from inpatient_episode_fall_codes
where admission_seq = 1),
---------------------BRADFORD FALLS-------------------------------------------------------------------------------------------------------------------
  bradford_episode_ch_cohort_ae as (
select
cast([person_id] as bigint) as person_id,
[tbl_ae_start_date],
[tbl_ae_end_date],
rtrim([diagnosis_1],'X') as diagnosis_1,
rtrim([diagnosis_2],'X') as diagnosis_2,
rtrim([diagnosis_3],'X') as diagnosis_3,
rtrim([diagnosis_4],'X') as diagnosis_4
from [bhts-connectyor].[src_StagingDatabase_Warehouse].[dbo].[tbl_ae]
where cast([person_id] as bigint) in(select cast(person_id as bigint) from [bhts-connectyo3].[CB_2172].[dbo].[CB_2172.care_home_cohort_v1])),

--joins care home entry date from the master cohort table
bradford_episode_entrydate_join_ae as (
  select
  e_c.*,
  convert(datetime,a.date_of_admission,103) ch_admission_date
  from bradford_episode_ch_cohort_ae as e_c
  left join [bhts-connectyo3].[CB_2172].[dbo].[CB_2172.care_home_cohort_v1] a
  on e_c.person_id  = cast(a.person_id as bigint) ),
-- filters out episodes which began after the admission date to a care home
bradford_episode_ch_filtered_ae as (
  select
e_e_j.*
  from bradford_episode_entrydate_join_ae e_e_j
  where (tbl_ae_start_date > ch_admission_date) AND (DATEDIFF(day, ch_admission_date,tbl_ae_start_date) < 366 )
),
-- filter for the first episode in a spell and extract the first three characters from each icd10 diagnosis code (not interested in the fourth character code for falls code)
bradford_episode_ch_final_ae as (
select
person_id,
ch_admission_date,
diagnosis_1,
diagnosis_2,
diagnosis_3,
tbl_ae_start_date,
tbl_ae_end_date,  
substring(diagnosis_1,1,3) as diagnosis_1_substr,
substring(diagnosis_2,1,3) as diagnosis_2_substr,
substring(diagnosis_3,1,3) as diagnosis_3_substr,
substring(diagnosis_4,1,3) as diagnosis_4_substr

from bradford_episode_ch_filtered_ae),
-- up to this point reprents the code to identify and clean all seocndary inpatient episodes for indiivudals in the connected bradford cohort in the 12 months after admission,the next line of code will identify admission related to falls, will then union this with a table for falls in AE. admissions related to falls was defined as any admission with a SNOMED OR ICD10 code available.

bradford_fall_ch_ae as (
select
*,
row_number() over (partition by person_id order by tbl_ae_start_date) as admission_seq
from bradford_episode_ch_final_ae
where bradford_episode_ch_final_ae.diagnosis_1_substr COLLATE Latin1_General_CS_AS in(select Code from  [bhts-connectyo3].[CB_2172].[dbo].[falls__fragilityfracture_icd10_20250130114221]) OR
bradford_episode_ch_final_ae.diagnosis_2_substr COLLATE Latin1_General_CS_AS in(select Code from  [bhts-connectyo3].[CB_2172].[dbo].[falls__fragilityfracture_icd10_20250130114221]) OR
diagnosis_3_substr COLLATE Latin1_General_CS_AS in(select Code from  [bhts-connectyo3].[CB_2172].[dbo].[falls__fragilityfracture_icd10_20250130114221]) OR
diagnosis_1_substr COLLATE Latin1_General_CS_AS in(select cast(code as char) from [bhts-connectyo3].[CB_2172].[dbo].[falls_snomed_20250130114221] ) or 
diagnosis_2_substr COLLATE Latin1_General_CS_AS in(select cast(code as char) from [bhts-connectyo3].[CB_2172].[dbo].[falls_snomed_20250130114221] ) or 
diagnosis_3_substr COLLATE Latin1_General_CS_AS in(select cast(code as char) from [bhts-connectyo3].[CB_2172].[dbo].[falls_snomed_20250130114221] ) or
diagnosis_4_substr COLLATE Latin1_General_CS_AS in(select cast([Code] as char) from [bhts-connectyo3].[CB_2172].[dbo].[falls__fragilityfracture_icd10_20250130114221])
),
-- this then filters for the first hopsital spell with a fall coded in any of the diagnosis codes. it is theoretically possible that a patient had an inpatient fall which was coded in the co-moribdities rather than the primary reason for presntation, but it is assumed the margin of error is relatively small.

bradford_fall_ae_final as (
select
*
from bradford_fall_ch_ae
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
rtrim(diagnosis_3,'X') as diagnosis_3,
rtrim(diagnosis_4,'X') as diagnosis_4
from [bhts-connectyor].[src_StagingDatabase_Warehouse].[dbo].[tbl_episode]
where person_id in(select cast(person_id as bigint) from [bhts-connectyo3].[CB_2172].[dbo].[CB_2172.care_home_cohort_v1]  )),

--joins care home entry date from the master cohort table
bradford_episode_entrydate_join as (
  select
  e_c.*,
  convert(datetime,a.date_of_admission, 103) as  ch_admission_date
  from bradford_episode_ch_cohort as e_c
  left join [bhts-connectyo3].[CB_2172].[dbo].[CB_2172.care_home_cohort_v1]  a
  on e_c.person_id  = cast(a.person_id as bigint) ),
-- filters out episodes which began after the admission date to a care home
bradford_episode_ch_filtered as (
  select
e_e_j.*
  from bradford_episode_entrydate_join e_e_j
  where (tbl_episode_start_date > ch_admission_date) AND (DATEDIFF(day,ch_admission_date,tbl_episode_start_date ) < 366 )
),
-- filter for the first episode in a spell and extract the first three characters from each icd10 diagnosis code (not interested in the fourth character code for falls code)
bradford_episode_ch_final as (
select
cast(person_id as bigint) as person_id,
ch_admission_date,
diagnosis_1,
diagnosis_2,
diagnosis_3,
diagnosis_4,
tbl_episode_start_date,
tbl_episode_end_date,  
substring(diagnosis_1,1,3) as diagnosis_1_substr,
substring(diagnosis_2,1,3) as diagnosis_2_substr,
substring(diagnosis_3,1,3) as diagnosis_3_substr,
substring(diagnosis_4,1,3) as diagnosis_4_substr
from bradford_episode_ch_filtered
where episode_sequence = 1
),
-- up to this point reprents the code to identify and clean all seocndary inpatient episodes for indiivudals in the connected bradford cohort in the 12 months after admission,the next line of code will identify admission related to falls, will then union this with a table for falls in AE. admissions related to falls was defined as any admission with a SNOMED OR ICD10 code available.

bradford_fall_ch as (
select
*,
row_number() over (partition by person_id order by tbl_episode_start_date) as admission_seq
from bradford_episode_ch_final
where bradford_episode_ch_final.diagnosis_1_substr COLLATE Latin1_General_CS_AS in(select Code from  [bhts-connectyo3].[CB_2172].[dbo].[falls__fragilityfracture_icd10_20250130114221]) 
OR bradford_episode_ch_final.diagnosis_2_substr COLLATE Latin1_General_CS_AS in(select Code from  [bhts-connectyo3].[CB_2172].[dbo].[falls__fragilityfracture_icd10_20250130114221]) 
OR bradford_episode_ch_final.diagnosis_3_substr COLLATE Latin1_General_CS_AS in(select Code from  [bhts-connectyo3].[CB_2172].[dbo].[falls__fragilityfracture_icd10_20250130114221]) 
or bradford_episode_ch_final.diagnosis_1_substr COLLATE Latin1_General_CS_AS in(select cast(code as char) from [bhts-connectyo3].[CB_2172].[dbo].[falls_snomed_20250130114221] ) 
or diagnosis_2_substr COLLATE Latin1_General_CS_AS in(select cast(code as char) from [bhts-connectyo3].[CB_2172].[dbo].[falls_snomed_20250130114221] ) 
or bradford_episode_ch_final.diagnosis_3_substr COLLATE Latin1_General_CS_AS in(select cast(code as char) from [bhts-connectyo3].[CB_2172].[dbo].[falls_snomed_20250130114221] )
or bradford_episode_ch_final.diagnosis_4_substr COLLATE Latin1_General_CS_AS in(select cast(code as char) from [bhts-connectyo3].[CB_2172].[dbo].[falls_snomed_20250130114221] )
),
-- this then filters for the first hopsital spell with a fall coded in any of the diagnosis codes. it is theoretically possible that a patient had an inpatient fall which was coded in the co-moribdities rather than the primary reason for presntation, but it is assumed the margin of error is relatively small.

bradford_fall_ch_inpatient as (
select
*
from bradford_fall_ch
where admission_seq = 1),

fall_bradford as (
select
cast(person_id as bigint) as person_id,
ch_admission_date,
tbl_episode_start_date
from bradford_fall_ch_inpatient

UNION

select
cast(person_id as bigint) as person_id,
ch_admission_date,
tbl_ae_start_date
from bradford_fall_ae_final),

-- then we union distinct inpatient episodes and ED episodes together before adding to airedale

fall_union as (
select
person_id,
ch_admission_date,
tbl_episode_start_date
from fall_bradford


union all

select
person_id,
fall_aire.ch_admission_date,
fall_aire.hopsital_attendance_date
from
fall_aire),

sequenced as(
select
*,
row_number() over (partition by person_id order by tbl_episode_start_date) admission_sequence
from fall_union)

select
* 
from sequenced
where admission_sequence = 1 
"

falls_care_home_cohort <- dbGetQuery(sql_conn,falls_query) 
colnames(falls_care_home_cohort) <- c("person_id","admission_date","fall.date")
#sswitch admission_date to date format
falls_care_home_cohort$admission_date <- falls_care_home_cohort$admission_date %>% as_date()
falls_care_home_cohort$fall.date <- falls_care_home_cohort$fall.date %>% as_date()
falls_care_home_cohort$survival_time <- falls_care_home_cohort$fall.date - falls_care_home_cohort$admission_date
falls_care_home_cohort <- subset(falls_care_home_cohort,survival_time <366) 

# pull in care home table

ch_query <- "select
*
from [cb_2172].[dbo].[cb_2172.care_home_cohort_v1]"

ch_cohort <- dbGetQuery(sql_conn,ch_query)

#create new variable date.event which is event time, either death, first fall event or end of FU (365 days), which ever comes sooner
ch_cohort$date.event <-as.POSIXct(ch_cohort$dod,format = "%d/%m/%Y") %>% as_date()
ch_cohort <- ch_cohort %>% select(-dod)
ch_cohort <- ch_cohort %>% arrange(person_id, desc(date.event)) %>% mutate(event_id= rank(date.event))

#join falls table and care home cohort maaster table 
ch_fall_merged <- merge(x =ch_cohort, y = falls_care_home_cohort[c(1,2,3,5)], all.x = TRUE) %>% select(-episodestartdate,-admission_date,-PostCodeDistrict)
#create exposure time variable to calculate incidence density
ch_fall_merged$admission_date <-as.POSIXct(ch_fall_merged$date_of_admission,format = "%d/%m/%Y") %>% as_date()

ch_fall_merged$exposure_time <- 0

for(i in 1:nrow(ch_fall_merged)) {
   if(!is.na(ch_fall_merged[i,]$fall.date)) {
     ch_fall_merged[i,]$exposure_time <- ch_fall_merged[i,]$fall.date - ch_fall_merged[i, ]$admission_date
   } else if (is.na(ch_fall_merged[i, ]$fall.date) & !is.na(ch_fall_merged[i, ]$date.event) & 
              ((ch_fall_merged[i, ]$date.event - ch_fall_merged[i, ]$admission_date) < 365)) {
     ch_fall_merged[i,]$exposure_time <- ch_fall_merged[i,]$date.event - ch_fall_merged[i,]$admission_date
   } else {
  ch_fall_merged[i,]$exposure_time <- 365
   }
}

ch_fall_merged$exposure_time <- as.numeric(ch_fall_merged$exposure_time)


#for those with missing death dates, it waa assumed they had 12 months of exposure
#set the exposure time for those alive more 12 months afterwards to 365 days to calculate incidence density in the year after admission

ch_fall_merged <- ch_fall_merged %>% mutate(fall = if_else(!is.na(fall.date),TRUE,FALSE))

#add hypertension diagnosis

hypertension_query <- "
SELECT  [person_id]
      ,[hypertension]
      ,[first_episodestartdate]
  FROM [CB_2172].[dbo].[cb_2172_hypertension_combined_20250130114221]"

ch_htn <- dbGetQuery(sql_conn,hypertension_query)

ch_fall_htn_merged <- merge(x=ch_fall_merged,y=ch_htn,by.x='person_id',by.y='person_id',all.x= TRUE) %>% select(-first_episodestartdate)

#then we caluclate the inciddence rate by dividing the total number of falls in the cohort by the exposure time in person years

incidnece_density_falls <- (sum(!is.na(ch_fall_merged$fall.date))/(ch_fall_merged$exposure_time %>% sum()/365))*100
