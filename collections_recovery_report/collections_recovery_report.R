source('S:/Risk Analytics/Configurations/config.r')

# Read in ach_change report from Temenos
ach_change <- read_csv('S:/Shared Departmental Reporting/Collections/Recovery Forecast Reports/input/ach_change_reporting.csv',
                       skip = 3) %>%
  clean_names()

########### Numeric
draws <- ach_change %>%
  select(account_identifier, ru_flag, contains('numberof_draws')) %>%
  pivot_longer(cols = -c(1,2), names_to = 'line', values_to = 'value') %>%
  mutate(Step = str_extract(line, '([0-9]+).*$')) %>%
  mutate(Measure = gsub('([0-9]+).*$', "", line)) %>%
  mutate(Measure = str_replace(Measure, 'ach_change_','')) %>%
  mutate(Measure = str_replace(Measure, '_step','')) %>%
  select(-line) %>%
  pivot_wider(names_from = 'Measure', values_from = 'value') 

########### Dollar formatted
money <- ach_change %>%
  select(account_identifier, ru_flag, contains('draw_amount'), contains('ttlam_tof_draws')) %>%
  pivot_longer(cols = -c(1,2), names_to = 'line', values_to = 'value') %>%
  mutate(Step = str_extract(line, '([0-9]+).*$'),
         value =  as.numeric(gsub('[$,]', '', value))) %>%
  mutate(Measure = gsub('([0-9]+).*$', "", line)) %>%
  mutate(Measure = str_replace(Measure, 'ach_change_','')) %>%
  mutate(Measure = str_replace(Measure, '_step',''))  %>%
  select(-line) %>%
  pivot_wider(names_from = 'Measure', values_from = 'value') 

########### Character 
frequency <- ach_change %>%
  select(account_identifier, ru_flag, contains('frequency')) %>%
  pivot_longer(cols = -c(1,2), names_to = 'line', values_to = 'value') %>%
  mutate(Step = str_extract(line, '([0-9]+).*$')) %>%
  mutate(Step = str_replace(Step, 'value','')) %>%
  mutate(Measure = gsub('([0-9]+).*$', "", line)) %>%
  mutate(Measure = str_replace(Measure, 'ach_change_','')) %>%
  mutate(Measure = str_replace(Measure, '_step','')) %>%
  select(-line) %>%
  pivot_wider(names_from = 'Measure', values_from = 'value') 

########### Character 
dates <- ach_change %>%
  select(account_identifier, ru_flag, contains('date'), -ach_change_create_date) %>%
  pivot_longer(cols = -c(1,2), names_to = 'line', values_to = 'value') %>%
  mutate(Step = str_extract(line, '([0-9]+).*$')) %>%
  mutate(Measure = gsub('([0-9]+).*$', "", line)) %>%
  mutate(Measure = str_replace(Measure, 'ach_change_','')) %>%
  mutate(Measure = str_replace(Measure, '_step','')) %>%
  mutate(value = mdy(value)) %>%
  select(-line) %>%
  pivot_wider(names_from = 'Measure', values_from = 'value') 


### bring together
holidays <- tbl(con, 'holidays') %>%
  collect() %>%
  clean_names()

bring_together <- draws %>%
  inner_join(money) %>%
  inner_join(frequency) %>%
  inner_join(dates) %>%
  filter(is.na(numberof_draws) == FALSE)%>%
  mutate(start_date = case_when(is.na(start_date)  == TRUE ~ lag(start_date, 1),
                                TRUE ~ start_date)) %>%
  mutate(new_start_date = case_when(
    day(start_date) == 31 ~ start_date - 1,
    TRUE ~ start_date), 
    
    new_end_date = case_when(
      frequency == 'Weekly' ~ start_date + (numberof_draws - 1) * 7,
      frequency == 'Monthly' ~ start_date %m+% months(numberof_draws - 1),
      frequency == 'Daily' ~ end_date,
      frequency == 'Bi-Weekly' ~ start_date + (numberof_draws-1) * 14,
      frequency == 'One Time' ~ start_date,
      TRUE ~ end_date),
    increment = case_when(
      frequency == 'Weekly' ~  7,
      frequency == 'Daily' ~  1,
      frequency == 'Bi-Weekly' ~ 14,
      frequency == 'One Time' ~ 0,
      frequency == 'Twice a Month' ~ 15, 
      TRUE ~ 0)) %>%
  group_by(account_identifier) 

monthly <- bring_together %>%
  filter(frequency == 'Monthly') %>%
  group_by(account_identifier, Step, draw_amount) %>%
  summarize(start=min(new_start_date),end=max(new_end_date)) %>%
  nest(data = c(start, end)) %>%
  mutate(date_expected = map(data, ~seq(unique(.x$start), unique(.x$end), by = "month"))) %>%
  unnest(cols = c(data, date_expected)) %>%
  mutate(date_expected = case_when(month(date_expected) %in% c(1, 3, 5, 7, 8, 10, 12) & day(date_expected) == 30 ~ date_expected + 1,
                                   TRUE ~ date_expected))%>%
  select(-start, -end)

one_time <- bring_together %>%
  filter(frequency == 'One Time') %>%
  select(account_identifier, Step, date_expected = start_date, draw_amount)

weekly <- bring_together %>%
  filter(frequency != 'Monthly' & frequency != 'One Time' & frequency != 'Daily') %>%
  group_by(account_identifier, Step, draw_amount) %>%
  summarize(start=min(new_start_date),end=max(new_end_date), increment = max(increment)) %>%
  nest(data = c(start, end, increment)) %>%
  mutate(date_expected = map(data, ~seq(unique(.x$start), unique(.x$end), .x$increment))) %>%
  unnest(cols = c(data, date_expected)) %>%
  select(-start, -end, -increment)

daily <- bring_together %>%
  filter(frequency == 'Daily') %>%
  group_by(account_identifier, Step, draw_amount) %>%
  summarize(start=min(new_start_date),end=max(new_end_date), increment = max(increment)) %>%
  nest(data = c(start, end, increment)) %>%
  mutate(date_expected = map(data, ~seq(unique(.x$start), unique(.x$end), .x$increment))) %>%
  unnest(cols = c(data, date_expected)) %>%
  select(-start, -end, -increment) %>%
  left_join(holidays, by = c('date_expected' = 'holiday_date')) %>%
  filter(is.na(holiday_name) == TRUE & weekdays(date_expected) !=  'Saturday' & 
           weekdays(date_expected) != 'Sunday') %>%
  select(-holiday_name)

all_together <- monthly %>%
  rbind(one_time) %>%
  rbind(weekly) %>%
  rbind(daily)

take_care_of_dates <- all_together %>%
  left_join(holidays, by = c('date_expected' = 'holiday_date')) %>%
  mutate(holiday_original_date = holiday_name,
         day_original_date = weekdays(date_expected)) %>%
  
  mutate(new_date = case_when(is.na(holiday_name) == FALSE ~ date_expected + days(1),
                              TRUE ~ date_expected)) %>%
  mutate(day_new_date = weekdays(new_date)) %>%
  mutate(newer_date = case_when(weekdays(new_date) == "Saturday" ~ new_date + days(2),
                                weekdays(new_date) == "Sunday" ~ new_date + days(1),
                                TRUE ~ new_date)) %>%
  mutate(day_newer_date = weekdays(newer_date)) %>%
  select(-holiday_name) %>%
  left_join(holidays, by = c('newer_date' = 'holiday_date')) %>%
  mutate(newest_date = case_when(is.na(holiday_name) == FALSE ~ newer_date + days(1),
                                 TRUE ~ newer_date),
         holiday_new_date = holiday_name) %>%
  mutate(day_newest_date = weekdays(newest_date))

minimum_report_date <- min(take_care_of_dates$date_expected)

#### Pull the amount paid from the payment history
phs_2 <- tbl(con, 'Report_Aspire_Payment_History') %>%
  collect()

na <- read_xlsx('S:/Data Analytics/Credit/All Non-Accrual Contracts.xlsx') %>%
  clean_names()

#### Filter Down data sets 
new_history <- phs_2 %>%
  ungroup() %>%
  clean_names() %>%
  inner_join(na %>%
               select(contract_id), by = 'contract_id') %>%
  filter(date_paid >= minimum_report_date & date_paid <= Sys.Date())

final_expected <- take_care_of_dates

monthly_expected <- final_expected %>%
  group_by(contract_id = account_identifier, date = as.Date(as.yearmon(newest_date))) %>%
  summarize(amount_expected = sum(draw_amount)) %>%
  mutate(level = 'Monthly')

daily_expected <- final_expected %>%
  ungroup() %>%
  select(contract_id = account_identifier, date = newest_date, 
         amount_expected = draw_amount) %>%
  mutate(level = 'Daily')

weekly_expected <- final_expected %>%
  ungroup() %>%
  group_by(year(newest_date), week(newest_date)) %>%
  mutate(date = as.Date(min(newest_date))) %>%
  ungroup() %>%
  group_by(contract_id = account_identifier, date) %>%
  summarize(amount_expected = sum(draw_amount)) %>%
  mutate(level = 'Weekly')

expected <- monthly_expected %>%
  rbind(weekly_expected) %>%
  rbind(daily_expected)

date_frame <- expected %>%
  ungroup() %>%
  group_by(contract_id) %>%
  summarize(start=min(date),end=max(date)) %>%
  nest(data = c(start, end)) %>%
  mutate(data = map(data, ~seq(unique(.x$start), unique(.x$end), "day"))) %>%
  unnest(data) %>%
  rename(date_frame = data)

################# actual
monthly_actual <- new_history %>%
  group_by(contract_id, date = as.Date(as.yearmon(date_paid))) %>%
  summarize(amount_paid = sum(amount_paid)) %>%
  mutate(level = 'Monthly')

daily_actual <- new_history %>%
  ungroup() %>%
  mutate(date = ymd(as.Date(date_paid))) %>%
  select(contract_id, date, 
         amount_paid = amount_paid) %>%
  mutate(level = 'Daily')

weekly_actual <- new_history %>%
  ungroup() %>%
  group_by(year(date_paid), week(date_paid)) %>%
  mutate(date = as.Date(min(date_paid))) %>%
  ungroup() %>%
  group_by(contract_id, date) %>%
  summarize(amount_paid = sum(amount_paid)) %>%
  mutate(level = 'Weekly')

actual <- monthly_actual %>%
  rbind(weekly_actual) %>%
  rbind(daily_actual)

################ non_accruals
monthly_actual_na <- new_history %>%
  inner_join(na %>%
               select(contract_id), by = 'contract_id') %>%
  anti_join(ach_change %>%
              select(account_identifier), by = c('contract_id' = 'account_identifier')) %>%
  group_by(date = as.Date(as.yearmon(date_paid))) %>%
  summarize(amount_paid = sum(amount_paid)) %>%
  mutate(level = 'Monthly')

daily_actual_na <- new_history %>%
  ungroup() %>%
  inner_join(na %>%
               select(contract_id), by = 'contract_id') %>%
  anti_join(ach_change %>%
              select(account_identifier), by = c('contract_id' = 'account_identifier')) %>%
  mutate(date = ymd(as.Date(date_paid))) %>%
  select( date,
          amount_paid = amount_paid) %>%
  mutate(level = 'Daily')

weekly_actual_na <- new_history %>%
  ungroup() %>%
  inner_join(na %>%
               select(contract_id), by = 'contract_id') %>%
  anti_join(ach_change %>%
              select(account_identifier), by = c('contract_id' = 'account_identifier')) %>%
  group_by(year(date_paid), week(date_paid)) %>%
  mutate(date = as.Date(min(date_paid))) %>%
  ungroup() %>%
  group_by( date) %>%
  summarize(amount_paid = sum(amount_paid)) %>%
  mutate(level = 'Weekly')

actual_na <- monthly_actual_na %>%
  rbind(weekly_actual_na) %>%
  rbind(daily_actual_na) %>%
  group_by(date, level) %>%
  summarize(amount_paid = sum(amount_paid))

################### New
date_frame_1 <-  date_frame %>%
  left_join(expected %>%
              group_by(contract_id), by = c('contract_id', 'date_frame' = 'date')) %>%
  rename(dollars = amount_expected) %>%
  mutate(measure = 'Expected', 
         date_frame = ymd(date_frame))

date_frame_2 <-  actual %>%
  inner_join(ach_change %>%
               select(account_identifier), by = c('contract_id' = 'account_identifier')) %>%
  rename(dollars = amount_paid) %>%
  mutate(measure = 'Actual', 
         date_frame = ymd(date)) %>%
  select(contract_id, date_frame, dollars, level, measure)

min_expected_date <- expected %>%
  group_by(contract_id) %>%
  summarize(min_date = ymd(min(date)))

final_data <- ach_change %>%
  rename(contract_id = account_identifier) %>%
  select(contract_id, full_name, eft, in_arrangement, ru_flag, contractual_attempt, queue_name, legal_flag) %>%
  inner_join(date_frame_1 %>%
               rbind(date_frame_2) , by = 'contract_id') %>%
  left_join(min_expected_date, by = 'contract_id') 


########### Write final files
final_data %>%
  filter(is.na(dollars) == FALSE) %>%
  write_csv('S:/Shared Departmental Reporting/Collections/Recovery Forecast Reports/output/collections_report_data.csv')

actual_na %>%
  filter(is.na(amount_paid) == FALSE) %>%
  write_csv('S:/Shared Departmental Reporting/Collections/Recovery Forecast Reports/output/collections_report_nas.csv')

print(paste("The Collections Recovery Report has been updated at ", Sys.time()))
