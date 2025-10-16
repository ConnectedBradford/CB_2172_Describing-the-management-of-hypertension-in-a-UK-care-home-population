library(readr)
library(DBI)
library(odbc)
library(dplyr)
library(forcats)
library(survival)
library(stats)
library(finalfit)
library(ggplot2)

sql_conn_omop <- dbConnect(odbc::odbc(),
                           driver = "SQL Server",
                           server = "bhts-connectyo3",
                           database = "CB_2172",
                           Trusted_Connection = "True")
query <- "
SELECT TABLE_SCHEMA, TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE'"

#load in care home cohort table

care_home_query <- "
select
*
from [bhts-connectyo3].[CB_2172].[dbo].[CB_2172.care_home_cohort_v1]"

care_home_table <- dbGetQuery(sql_conn_omop,care_home_query)

#create survival time variable 
care_home_table$mortality <- care_home_table$mortality %>% as.numeric()
care_home_table <- care_home_table %>% mutate(survival_time = coalesce(mortality,fu_duration)) 
care_home_table$survival_time <- as.numeric(care_home_table$survival_time)

care_home_table$end_of_fu_status <- as.factor(care_home_table$end_of_fu_status) %>% fct_recode("0" = "survived", "1" = "died", "2" = "lost to follow up") %>%  as.character() %>% as.numeric() 

survival_object_months <- Surv(time = (care_home_table$survival_time/(365/12)), event = care_home_table$end_of_fu_status)

survival_object_years <- Surv(time = (care_home_table$survival_time/(365)), event = care_home_table$end_of_fu_status)

my_survfit <- survfit(survival_object_months ~ 1, data = care_home_table) 

plot(my_survfit, main = "Survival amongst care home residents following admission", xlab = "Time (months)", ylab = "Survival Probability", xlim = c(0, 80), col = "blue", lwd = 2)
care_home_table$year_of_admission <- care_home_table$date_of_admission %>% as.Date(format ="%d/%m/%Y") %>% format("%Y")

care_home_table %>% surv_plot(survival_object_months,explanatory, pval = TRUE)

#sensitivity analysis to determine if survival time differecnes significantly according to year of admission to account 
# for effects of COVID-19

survdiff(Surv(time = (care_home_table$survival_time/(365)), event = care_home_table$end_of_fu_status) ~ year_of_admission, data = care_home_table)

#cox ph analysis 

coxph(Surv(time = (care_home_table$survival_time/(365)), event = care_home_table$end_of_fu_status) ~ year_of_admission, data = care_home_table) |> 
  tbl_regression(exp = TRUE) 

##### mortality analysis- limited to 2 year follow-up ####
#same analysis but follow-up limited to 2 years after care home admission
care_home_table$survival_time_2_years <- 0
care_home_table$end_of_fu_status_2_year <- 0
for(i in 1:nrow(care_home_table)) {
  # Case 1: If end_of_fu_status is 0 or mortality > 730
  if(care_home_table[i, "end_of_fu_status"] == 0 | care_home_table[i, "mortality"] > 730) {
    care_home_table[i, "survival_time_2_years"] <- 730
    care_home_table[i, "end_of_fu_status_2_year"] <- 0
    
    # Case 2: If end_of_fu_status is 1 and mortality < 730
  } else if(care_home_table[i, "end_of_fu_status"] == 1 && care_home_table[i, "mortality"] < 730) {
    care_home_table[i, "survival_time_2_years"] <- care_home_table[i, "mortality"]
    care_home_table[i, "end_of_fu_status_2_year"] <- 1
    
    # Optional: Handle all other cases explicitly
  } else {
    care_home_table[i, "survival_time_2_years"] <- NA
    care_home_table[i, "end_of_fu_status_2_year"] <- NA
  }
}

#generate survival function
survival_object_months <- Surv(time = (care_home_table$survival_time_2_years/(365/12)), event = care_home_table$end_of_fu_status_2_year)

survival_object_years <- Surv(time = (care_home_table$survival_time_2_years/(365)), event = care_home_table$end_of_fu_status_2_year)
#cox regression analysis to determine if survival time differ sby year of admission when limited to 2 year follow-up
coxph(Surv(time = (care_home_table$survival_time_2_years/(365)), event = care_home_table$end_of_fu_status_2_year) ~ admission_service + gender + year_of_admission, data = care_home_table) 
   %>% summary()
