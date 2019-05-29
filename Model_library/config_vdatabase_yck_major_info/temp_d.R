#全量更新分析宽表analysis_wide_table的数据（临时建立）
rm(list = ls(all=T))
gc()
library(dplyr)
library(stringr)
library(raster)
library(RMySQL)
library(reshape2)
local_file<-gsub("\\/bat|\\/main.*","",tryCatch(dirname(rstudioapi::getActiveDocumentContext()$path),error=function(e){getwd()}))
source(paste0(local_file,"\\config\\config_fun\\fun_mysql_config_up.R"),echo=FALSE,encoding="utf-8")
local_defin<-fun_mysql_config_up()$local_defin
local_defin_yun<-fun_mysql_config_up()$local_defin_yun
data_new<-Sys.Date()%>%as.character()
#步骤一
loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
dbSendQuery(loc_channel,'SET NAMES gbk')
analysis_wide_table<-dbFetch(dbSendQuery(loc_channel,"SELECT * FROM analysis_wide_table;"),-1) %>% 
  dplyr::select(-model_year,-brand,-series,-model_name,-liter,-auto,-discharge_standard,-car_level,-regional)
analysis_wide_table_copy<-dbFetch(dbSendQuery(loc_channel,"SELECT * FROM analysis_wide_table_copy limit 10;"),-1)
# rm_series_rule<-dbFetch(dbSendQuery(loc_channel,"SELECT * FROM config_reg_series_rule;"),-1)
che300<-dbFetch(dbSendQuery(loc_channel,"SELECT * FROM config_vdatabase_yck_major_info;"),-1) %>% 
  dplyr::select(model_id,model_year,yck_brandid,yck_seriesid,is_import,is_green,brand=brand_name,series=series_name,model_name,liter,auto,discharge_standard,car_level)
dbDisconnect(loc_channel)

a<-names(analysis_wide_table_copy)
analysis_wide_table_copy<-inner_join(analysis_wide_table,che300,by=c("id_che300"='model_id')) %>% dplyr::select(a)
rm(analysis_wide_table)
gc()
#清洗
analysis_wide_table_copy$trans_fee<-gsub("NA","",analysis_wide_table_copy$trans_fee)
analysis_wide_table_copy$insure<-gsub("NA","",analysis_wide_table_copy$insure)
analysis_wide_table_copy$annual<-gsub("NA","",analysis_wide_table_copy$annual)
analysis_wide_table_copy$transfer<-gsub("暂0数据","",analysis_wide_table_copy$transfer)

n_total<-500000
n_count<-floor(nrow(analysis_wide_table_copy)/n_total)
t1<-Sys.time()
for (i in 1:n_count) {
  t1<-Sys.time()
  ana_temp<-analysis_wide_table_copy[(n_total*(i-1)+1):(n_total*i),]
  write.csv(ana_temp,paste0(local_file,"analysis_wide_table.csv",sep=""),
            row.names = F,fileEncoding = "UTF-8",quote = F)
  loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
  dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE '",local_file,"analysis_wide_table.csv'",
                                 " INTO TABLE analysis_wide_table_copy CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;",sep=""))
  dbDisconnect(loc_channel)
  Sys.time()-t1
}

if(nrow(analysis_wide_table_copy)%%n_total!=0){
  ana_temp<-analysis_wide_table_copy[(n_total*n_count+1):nrow(analysis_wide_table_copy),]
  write.csv(ana_temp,paste0(local_file,"analysis_wide_table.csv",sep=""),
            row.names = F,fileEncoding = "UTF-8",quote = F)
  loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
  dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE '",local_file,"analysis_wide_table.csv'",
                                 " INTO TABLE analysis_wide_table_copy CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;",sep=""))
  dbDisconnect(loc_channel)
}