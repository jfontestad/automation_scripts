#This script is used to manage the scheduled scripts 
source('S:/Risk Analytics/Configurations/config.r')
library(taskscheduleR) 


taskscheduler_create(taskname = "third_party_data", 
                     rscript = 'S:/Risk Analytics/Automated Scripts/get_attribute_reports/get_attribute_reports.r', 
                     schedule = "DAILY", 
                     starttime = "23:00", 
                     startdate = "05/25/2021")

taskscheduler_create(taskname = "collections_recovery_report", 
                     rscript = 'S:/Risk Analytics/Automated Scripts/collections_recovery_report/collections_recovery_report.r', 
                     schedule = "DAILY", 
                     starttime = "00:00", 
                     startdate = "05/26/2021")

# taskscheduler_delete(taskname = 'third_party_data')
# taskscheduler_delete(taskname = 'collections_recovery_report')

tasks <- taskscheduler_ls()

tasks %>%
  View()

taskscheduleR::taskscheduler_runnow('third_party_data')
taskscheduleR::taskscheduler_runnow('collections_recovery_report')
