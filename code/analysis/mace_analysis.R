# script to perform analysis of MACE events in care home cohort 
#load packages
library(ggplot2)
library(odbc)
library(DBI)
library(tidyverse)
library(lubridate)
library(stats)
library(epitools)

#connect to sql tables 
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

#pull in mace tables and care home table 

mace_query <-  "
with care_home_cohort as (
SELECT [person_id],CONVERT(datetime,episodestartdate,102) as ch_admission_date,[dod]
  FROM [CB_2172].[dbo].[care_home_cohort_v1_20250130114221]
  where person_id in (SELECT [person_id]
  FROM [CB_2172].[dbo].[cb_2172_hypertension_combined_20250130114221]
  where hypertension = 1)
),

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
where person_id in(select person_id from care_home_cohort)),
-- for some reason the airedale hospital records are split into pre and post 2019 so i had to combine the two tables toether to ensure no episodes were missed




-- joins care home entry date from the master cohort table
episode_entrydate_join as (
  select
  e_c.person_id,
  convert(datetime,e_c.episode_start,120) as hopsital_attendance_date,
  ch_admission_date,
  episode_sequence,
  diagnosis_1,
  diagnosis_2,
  diagnosis_3,
  diagnosis_4,
  episode_end
  from episode_ch_cohort_2015 as e_c
  left join care_home_cohort  a
  on e_c.person_id = a.person_id),
-- filters out episodes which began after the admission date to a care home
episode_ch_filtered as (
  select
e_e_j.*
  from episode_entrydate_join e_e_j
  where (hopsital_attendance_date > ch_admission_date) AND (DATEDIFF(day,hopsital_attendance_date,ch_admission_date) < 366)
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

-- identify mi and stroke episodes 
mi_airedale_2015_2019 as (
select
*
from inpatient_episode_ch_final
where diagnosis_1_substr in ('I21','I22','I23') OR
diagnosis_2_substr in ('I21','I22','I23') OR
diagnosis_3_substr in ('I21','I22','I23') OR 
diagnosis_4_substr in ('I21','I22','I23') 
),

stroke_airedale_2015_2019 as (
select
* from inpatient_episode_ch_final
where diagnosis_1_substr in (select
icd from [cb_2172].[dbo].stroke_icd10_code_20250130114221) OR
diagnosis_2_substr in (select
icd from [cb_2172].[dbo].stroke_icd10_code_20250130114221) OR
diagnosis_3_substr in (select
icd from [cb_2172].[dbo].stroke_icd10_code_20250130114221) OR 
diagnosis_4_substr in (select
icd from [cb_2172].[dbo].stroke_icd10_code_20250130114221)),

mace_updated_ch_cohort as (
select
person_id,hopsital_attendance_date,
'myocardial infarction' event_type 
from mi_airedale_2015_2019

union all

select
person_id,hopsital_attendance_date ,
'stroke' as event_type
from stroke_airedale_2015_2019

union all 

SELECT [person_id]
      ,[dod]
      ,[event_type]
  FROM [CB_2172].[dbo].[cb_2172_mace_20250130114221]),

  mace_updated_ch_cohort_numbered as (
  select
  *,
  row_number() over(partition by person_id order by hopsital_attendance_date) as admission_sequence 
  from mace_updated_ch_cohort),
  
  mace as (
  select
  * 
  --into [cb_2172].[dbo].[cb_2172_mace_v2]
  from mace_updated_ch_cohort_numbered 
)

SELECT  

a.hopsital_attendance_date as hospital_attendance_date,
ch_admission_date,a.person_id,b.dod as death_date,
event_type
from mace a
left join care_home_cohort b 
on a.person_id = b.person_id
where a.person_id in(select person_id from care_home_cohort )
;"

mace_data <- dbGetQuery(sql_conn,mace_query)

#rank mace events for each person so we can identify first event
mace_data_event_ranked <- mace_data %>% 
  arrange(person_id, desc(hospital_attendance_date)) %>% 
  mutate(event_id= rank(hospital_attendance_date))
#here is the initial data, the problem is there are some  duplicate events where the data differs
#by only a couple of days so they are counted as different events. 
#Some cardiovascular deaths also have  the cardiovascular event (e.g. myocardial evvent,
#recorded as a seperate event a few days apart). I need to remove these events so that only the cardiovascular death is recorded. 
#This is based on the assumption that where a stroke or MI has been recorded within 30 days of a cardiovascular death, 
# the stroke or MI can be considered fatal one so should be recorded as a CV death 
#rather than a non fatal MI or stroke. 

#convert varaibles to dates 
mace_data_event_ranked$date.event <- as_date(mace_data_event_ranked$hospital_attendance_date)
mace_data_event_ranked$ch_admission_date <- as_date(mace_data_event_ranked$ch_admission_date)
mace_data_event_ranked$death_date <- as_date(mace_data_event_ranked$death_date)

#remove mace events that occurred at same time as care home admission since these are assumed to have happened before care home admission
mace_data_event_ranked <- mace_data_event_ranked %>% filter(date.event > ch_admission_date) %>% select(-hospital_attendance_date)


# this identifies myocardial infarction events where another event occurs afterwards
# (either a duplicate event or death), so these myocardial events can be removed, 
#i will do the same for stroke as well. this ensures only non fatal myocardial infarctions 
# and strokes are captured in the MACE outcomes. fatal myocardial infarctions and stroke would fall into CV death 
mace_data_filtered_mi_cvd <- mace_data_event_ranked %>%
  group_by(person_id) %>% 
  filter(event_type == 'myocardial infarction' & 
           !((date.event - lag(date.event)) >= 30 &
               (date.event - lag(date.event)) > 0)) %>%
  ungroup() 

mace_data_filtered_mi_cvd
# this identifies stroke events where another event occurs afterwards(either a duplicate event or death), so these myocardial events can be removed, i will do the same for stroke as well.
#this ensures only non fatal myocardial infarctions and strokes are captured in the MACE outcomes.
#fatal myocardial infarctions and stroke would fall into CV death 
mace_filtered_stroke_cvd <- mace_data_event_ranked %>%
  group_by(person_id) %>%
  filter((event_type == 'stroke') & 
           !((date.event - lag(date.event)) >= 30 &
               (date.event - lag(date.event)) > 0)) %>%
  ungroup() 

mace_filtered_stroke_cvd

#i then subsetted the mace_Data to remove those duplicate stroke and myocardial events identified above


#creat he not in function

'%notin%' <- Negate(`%in%`)

mace_data_filtered <- mace_data_event_ranked[mace_data_event_ranked$event_id %notin% mace_filtered_stroke_cvd$event_id, ]

mace_data_filtered <- mace_data_filtered[mace_data_filtered$date.event %notin% mace_filtered_stroke_cvd$date.event,]

mace_data_filtered <- mace_data_filtered %>% arrange(person_id, desc(date.event)) 

table(mace_data_filtered$event_type)

# merge the care home cohort table in order to calculate the incidence density
#(need to subset by the cae_home_cohort_v1 table as cohort was updated in feb 2025, to exclude people with no follow-up 


care_home_query <- "SELECT [person_id],CONVERT(datetime,episodestartdate,102) as ch_admission_date,[dod]
  FROM [CB_2172].[dbo].[care_home_cohort_v1_20250130114221]
  where person_id in (SELECT [person_id]
  FROM [CB_2172].[dbo].[cb_2172_hypertension_combined_20250130114221]
  where hypertension = 1);"

care_home_cohort <- dbGetQuery(sql_conn,care_home_query)

ch_mace_merged <- merge(x =care_home_cohort, y = mace_data_filtered, all.x = TRUE)
ch_mace_merged$dod <- as_date(ch_mace_merged$dod)
ch_mace_merged$ch_admission_date <- as_date(ch_mace_merged$ch_admission_date)
ch_mace_merged<- ch_mace_merged %>% select(-death_date)
ch_mace_merged <- ch_mace_merged %>% mutate(exposure_time= 0)

# write a for if loop so that if the event_type occurred put that as the exposure time, but if it didn't occur check whether death date occured within 12 months, if it did
# use the death to calculate exposure time if it didn't then put the expsoure time as 365

for(i in 1:nrow(ch_mace_merged)) {
  if(!is.na(ch_mace_merged[i,]$event_type)) {
    ch_mace_merged[i,]$exposure_time <- ch_mace_merged[i,]$date.event - ch_mace_merged[i,]$ch_admission_date
  } else if (is.na(ch_mace_merged[i,]$event_type)
             & !is.na((ch_mace_merged[i,]$dod)) & ((ch_mace_merged[i,]$dod - ch_mace_merged[i,]$ch_admission_date) < 365)) 
    {
    ch_mace_merged[i,]$exposure_time <- (ch_mace_merged[i,]$dod - ch_mace_merged[i,]$ch_admission_date)
  } else {
    ch_mace_merged[i,]$exposure_time <- 365
  }
}
  #calculate incidence rate density 
(sum(!is.na(ch_mace_merged$event_type))/(sum(ch_mace_merged$exposure_time)/365))*100

ch_mace_merged$event_type <- as.factor(ch_mace_merged$event_type)

exposure_time <- sum(ch_mace_merged$exposure_time)/365

mace_events_count<- sum(!is.na(ch_mace_merged$event_type))

pois.exact(mace_events_count, pt = exposure_time, conf.level = 0.95)


######### sensitivity analysis for COVID-19#######################
#this sensitivity analysis will determine whether the incidence rate varied significantly for those
# inidivudals whose exposure period included the months between march 2020 and december 2020 as hospital attendance 
#may have been significnatly lower in the care home population

#subdivide population in two groups accroding to admission date before and after june 2019 

ch_mace_pre_june_19 <- ch_mace_merged %>% filter(ch_admission_date < '2019-06-01')

ch_mace_post_june_19 <- ch_mace_merged %>% filter(ch_admission_date > '2019-06-01')

#incidenc density pre june 2019 

(sum(!is.na(ch_mace_pre_june_19$event_type))/(sum(ch_mace_pre_june_19$exposure_time)/365))*100

ch_mace_pre_june_19$event_type <- as.factor(ch_mace_pre_june_19$event_type)

exposure_time_1 <- sum(ch_mace_pre_june_19$exposure_time)/365

mace_events_count_1<- sum(!is.na(ch_mace_pre_june_19$event_type))

pois.exact(mace_events_count, pt = exposure_time, conf.level = 0.95)

#incidence density post june 2019 

(sum(!is.na(ch_mace_post_june_19$event_type))/(sum(ch_mace_post_june_19$exposure_time)/365))*100

ch_mace_post_june_19$event_type <- as.factor(ch_mace_post_june_19$event_type)

exposure_time_2 <- sum(ch_mace_post_june_19$exposure_time)/365

mace_events_count_2<- sum(!is.na(ch_mace_post_june_19$event_type))

pois.exact(mace_events_count, pt = exposure_time, conf.level = 0.95)

#poission test(exact) to determine difference between pre and post june 2019 
poisson.test(c(mace_events_count_1,mace_events_count_2),c(exposure_time_1,exposure_time_2))
