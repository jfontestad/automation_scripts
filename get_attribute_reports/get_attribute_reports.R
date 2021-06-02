source('S:/Risk Analytics/Configurations/config.r')

paynet_previous_wc <- read_csv('S:/Risk Analytics/data_mart/third_party_data/paynet/wc_paynet_report_full.csv')
# paynet_previous_ef <- read_csv('S:/Risk Analytics/data_mart/ef_paynet_report_full.csv') 

experian_previous_wc <- read_csv('S:/Risk Analytics/data_mart/third_party_data/experian/wc_experian_report_full.csv')
experian_previous_ef <- read_csv('S:/Risk Analytics/data_mart/third_party_data/experian/ef_experian_report_full.csv')

lnsbfe_previous_wc <- read_csv('S:/Risk Analytics/data_mart/third_party_data/lnsbfe/wc_lnsbfe_report_full.csv')
lnsbfe_previous_ef <- read_csv('S:/Risk Analytics/data_mart/third_party_data/lnsbfe/ef_lnsbfe_report_full.csv')

max_date_exp <- max(experian_previous_wc$pull_date)
max_date_paynet <- max(paynet_previous_wc$pull_date)
max_date_lnsbfe <- max(lnsbfe_previous_wc$pull_date)

get_experian <- query_documents(cosmos, paste("select r.id, r.PullDate, r.Type, r.ReportIndexId, 
                                  r.ReportDetail.Attributes FROM Reports AS r 
                                  where  r.PullDate > '", max_date_exp, "' and r.Type = 0", sep = ''))

get_paynet <- query_documents(cosmos, paste("select r.id, r.PullDate, r.Type, r.ReportIndexId, 
                                              r.ReportDetail.Attributes FROM Reports AS r 
                                              where  r.PullDate > '", max_date_paynet, "' and r.Type = 5", sep = ''))

get_lnsbfe <- query_documents(cosmos, paste("select r.id,r.PullDate, r.Type, r.ReportIndexId, 
                                  r.ReportDetail.Attributes FROM Reports AS r 
                                  where  r.PullDate > '", max_date_lnsbfe, "' and r.Type = 6", sep = ''))

get_experian <- do.call(data.frame, get_experian) %>%
  clean_names()

get_paynet <- do.call(data.frame, get_paynet) %>%
  clean_names()

get_lnsbfe <- do.call(data.frame, get_lnsbfe) %>%
  clean_names()

################################### Get join to opportunity
library(sqldf)

################## WC
wc_lexis <- dbGetQuery(con, "
select c.report_index_id, o.salesforce_account_id,   o.id as opportunity_id,  
      ri.unique_id_hint, ri.pull_date, ri.Document_Id, rt.description
from dbo.score_report c 
inner join dbo.score_request b
    on c.score_request_id = b.id
                                      inner join dbo.opportunity o
    on b.salesforce_opportunity_id = o.salesforce_id
                                      inner join dbo.report_index ri
    on c.report_index_id = ri.id
                                      inner join dbo.Report_type rt
    on ri.type = rt.id and rt.description = 'Lexis : SBFE'")

wc_lexis_max <- sqldf('select opportunity_id, unique_id_hint, max(pull_date) as pull_date 
from wc_lexis
group by opportunity_id, unique_id_hint')

wc_attr_lexis_riid <- sqldf('select distinct b.description, b.pull_date, b.report_index_id, b.salesforce_account_id, b.opportunity_id, a.Unique_Id_Hint,
       b.Document_Id 
       from wc_lexis_max a inner join wc_lexis b
    on a.opportunity_id = b.opportunity_id and a.unique_id_hint = b.unique_id_hint and a.pull_date = b.pull_date') %>%
  clean_names()

wc_experian <- dbGetQuery(con, "
select rt.description, pull_date, o.salesforce_account_id, o.id as opportunity_id, c.report_index_id, ri.unique_id_hint,
       ri.Document_Id
from dbo.score_report c inner join dbo.score_request b
    on c.score_request_id = b.id
                                      inner join dbo.opportunity o
    on b.salesforce_opportunity_id = o.salesforce_id
                                      inner join dbo.report_index ri
    on c.report_index_id = ri.id
                                      inner join dbo.Report_type rt
    on ri.type = rt.id and rt.description = 'Experian : Consumer Bureau'")

wc_experian_max <- sqldf('select opportunity_id, unique_id_hint, max(pull_date) as pull_date 
from wc_experian
group by opportunity_id, unique_id_hint')

wc_attr_exp_riid <- sqldf('select distinct b.description, b.pull_date, b.report_index_id, b.salesforce_account_id, b.opportunity_id, a.Unique_Id_Hint,
       b.Document_Id 
       from wc_experian_max a inner join wc_experian b
    on a.opportunity_id = b.opportunity_id and a.unique_id_hint = b.unique_id_hint and a.pull_date = b.pull_date') %>%
  clean_names()

wc_paynet <- dbGetQuery(con, "
select rt.description, pull_date, o.salesforce_account_id, o.id as opportunity_id, c.report_index_id, ri.unique_id_hint,
       ri.Document_Id
from dbo.score_report c inner join dbo.score_request b
    on c.score_request_id = b.id
                                      inner join dbo.opportunity o
    on b.salesforce_opportunity_id = o.salesforce_id
                                      inner join dbo.report_index ri
    on c.report_index_id = ri.id
                                      inner join dbo.Report_type rt
    on ri.type = rt.id and rt.description = 'PayNet'")

wc_paynet_max <- sqldf('select opportunity_id, unique_id_hint, max(pull_date) as pull_date 
from wc_paynet
group by opportunity_id, unique_id_hint')

wc_attr_paynet_riid <- sqldf('select distinct b.description, b.pull_date, b.report_index_id, b.salesforce_account_id, b.opportunity_id, a.Unique_Id_Hint,
       b.Document_Id 
       from wc_paynet_max a inner join wc_paynet b
    on a.opportunity_id = b.opportunity_id and a.unique_id_hint = b.unique_id_hint and a.pull_date = b.pull_date') %>%
  clean_names()

################## EF 

ef_lexis <- dbGetQuery(con, "
select rt.description, pull_date, o.salesforce_account_id, o.id as opportunity_id, c.report_index_id, ri.unique_id_hint,
       ri.Document_Id
from dbo.score_report c inner join dbo.score_request b
    on c.score_request_id = b.id
                                      inner join ef.opportunity o
    on b.salesforce_opportunity_id = o.salesforce_id
                                      inner join dbo.report_index ri
    on c.report_index_id = ri.id
                                      inner join dbo.Report_type rt
    on ri.type = rt.id and rt.description = 'Lexis : SBFE'")

ef_lexis_max <- sqldf('select opportunity_id, unique_id_hint, max(pull_date) as pull_date 
from ef_lexis
group by opportunity_id, unique_id_hint')

ef_attr_lexis_riid <- sqldf('select distinct b.description, b.pull_date, b.report_index_id, b.salesforce_account_id, b.opportunity_id, a.Unique_Id_Hint,
       b.Document_Id 
       from ef_lexis_max a inner join ef_lexis b
    on a.opportunity_id = b.opportunity_id and a.unique_id_hint = b.unique_id_hint and a.pull_date = b.pull_date') %>%
  clean_names()

ef_experian <- dbGetQuery(con, "
select rt.description, pull_date, o.salesforce_account_id, o.id as opportunity_id, c.report_index_id, ri.unique_id_hint,
       ri.Document_Id
from dbo.score_report c inner join dbo.score_request b
    on c.score_request_id = b.id
                                      inner join ef.opportunity o
    on b.salesforce_opportunity_id = o.salesforce_id
                                      inner join dbo.report_index ri
    on c.report_index_id = ri.id
                                      inner join dbo.Report_type rt
    on ri.type = rt.id and rt.description = 'Experian : Consumer Bureau'")

ef_experian_max <- sqldf('select opportunity_id, unique_id_hint, max(pull_date) as pull_date 
from ef_experian
group by opportunity_id, unique_id_hint')

ef_attr_exp_riid <- sqldf('select distinct b.description, b.pull_date, b.report_index_id, b.salesforce_account_id, b.opportunity_id, a.Unique_Id_Hint,
       b.Document_Id 
       from ef_experian_max a inner join ef_experian b
    on a.opportunity_id = b.opportunity_id and a.unique_id_hint = b.unique_id_hint and a.pull_date = b.pull_date') %>%
  clean_names()


########################## join
ef_join_exp <- ef_attr_exp_riid %>%   
  select(-pull_date) %>%
  inner_join(get_experian, by = c('document_id' = 'report_index_id'))

ef_join_lnsbfe <- ef_attr_lexis_riid %>%     
  select(-pull_date) %>%
  inner_join(get_lnsbfe, by = c('document_id' = 'report_index_id'))

wc_join_exp <- wc_attr_exp_riid %>%   
  select(-pull_date) %>%
  inner_join(get_experian, by = c('document_id' = 'id'))

wc_join_lnsbfe <- wc_attr_lexis_riid %>%   
  select(-pull_date) %>%
  inner_join(get_lnsbfe, by = c('document_id' = 'report_index_id'))

wc_join_paynet <- wc_attr_paynet_riid %>%   
  select(-pull_date) %>%
  inner_join(get_paynet, by = c('document_id' = 'report_index_id'))

wc_full <- wc_join_exp %>%
  left_join(wc_join_lnsbfe %>%
              select(-pull_date), by = 'opportunity_id') %>%
  left_join(wc_join_paynet %>%
              select(-pull_date), by = 'opportunity_id') %>%
  rbind(experian_previous_wc %>%
          left_join(lnsbfe_previous_wc %>%
                      select(-pull_date), by = 'opportunity_id') %>%
          left_join(paynet_previous_wc %>%
                      select(-pull_date), by = 'opportunity_id'))

ef_full <- ef_join_exp %>%
  left_join(ef_join_lnsbfe %>%
              select(-pull_date), by = 'opportunity_id') %>%
  rbind(experian_previous_ef %>%
          left_join(lnsbfe_previous_ef %>%
                      select(-pull_date), by = 'opportunity_id')) 


write_csv(experian_previous_ef, 'S:/Risk Analytics/data_mart/third_party_data/experian/ef_experian_report_previous.csv')
write_csv(lnsbfe_previous_ef, 'S:/Risk Analytics/data_mart/third_party_data/lnsbfe/ef_lnsbfe_report_previous.csv')
write_csv(experian_previous_wc, 'S:/Risk Analytics/data_mart/third_party_data/experian/wc_experian_report_previous.csv')
write_csv(lnsbfe_previous_wc, 'S:/Risk Analytics/data_mart/third_party_data/lnsbfe/wc_lnsbfe_report_previous.csv')
write_csv(paynet_previous_wc, 'S:/Risk Analytics/data_mart/third_party_data/paynet/wc_paynet_report_previous.csv')

write_csv(ef_join_exp, 'S:/Risk Analytics/data_mart/third_party_data/experian/ef_experian_report_full.csv')
write_csv(ef_join_lnsbfe, 'S:/Risk Analytics/data_mart/third_party_data/lnsbfe/ef_lnsbfe_report_full.csv')
write_csv(wc_join_exp, 'S:/Risk Analytics/data_mart/third_party_data/experian/wc_experian_report_full.csv')
write_csv(wc_join_lnsbfe, 'S:/Risk Analytics/data_mart/third_party_data/lnsbfe/wc_lnsbfe_report_full.csv')
write_csv(wc_join_paynet, 'S:/Risk Analytics/data_mart/third_party_data/paynet/wc_paynet_report_full.csv')

write_csv(ef_full, 'S:/Risk Analytics/data_mart/third_party_data/ef_reports_full.csv')
write_csv(wc_full, 'S:/Risk Analytics/data_mart/third_party_data/wc_reports_full.csv')

print(paste("The Bureau Attributes have been updated at ", Sys.time()))