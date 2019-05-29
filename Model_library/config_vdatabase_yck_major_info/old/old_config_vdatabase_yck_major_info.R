##-------------仅第一次全量使用部分-----------##
##---本代码处理车300车型库构建YCK车型库---#
rm(list = ls(all=T))
gc()
library(reshape2)
library(dplyr)
library(RMySQL)
library(stringr)
library(tcltk)
library(truncnorm)
library(rlist)
deep_local<-gsub("\\/main|\\/bat","",tryCatch(dirname(rstudioapi::getActiveDocumentContext()$path),error=function(e){getwd()}))
# model_id,Initial,brandid,brand_name,series_group_name,series_id,series_name,
# model_name,model_price,model_year,auto,liter,discharge_standard,max_reg_year,min_reg_year,car_level,seat_number,hl_configs,
# hl_configc,is_green
local_defin<-data.frame(user = 'root',host='192.168.0.111',password= '000000',dbname='yck-data-center',stringsAsFactors = F)
loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
dbSendQuery(loc_channel,'SET NAMES gbk')
config_che300_major_info<-dbFetch(dbSendQuery(loc_channel,"SELECT model_id,Initial,brandid,brand_name,series_group_name,series_id,series_name,
              model_name,model_price,model_year,auto,liter,discharge_standard,max_reg_year,min_reg_year,car_level,seat_number,hl_configs,
              hl_configc,is_green FROM config_che300_major_info;"),-1)
dbDisconnect(loc_channel)
#
config_vdatabase_yck_major_info<-config_che300_major_info
# config_vdatabase_yck_major_info$series_group_name[grep('进口',config_vdatabase_yck_major_info$model_name)]<-
#   paste0('进口',config_vdatabase_yck_major_info$series_group_name[grep('进口',config_vdatabase_yck_major_info$model_name)])
config_vdatabase_yck_major_info$series_group_name[grep('进口',config_vdatabase_yck_major_info$series_name)]<-
  paste0('进口',config_vdatabase_yck_major_info$series_group_name[grep('进口',config_vdatabase_yck_major_info$series_name)])
config_vdatabase_yck_major_info$series_group_name<-gsub('进口进口进口|进口进口','进口',config_vdatabase_yck_major_info$series_group_name)
config_vdatabase_yck_major_info$series_name<-gsub('\\(进口)','',config_vdatabase_yck_major_info$series_name)
config_vdatabase_yck_major_info$series_name<-toupper(config_vdatabase_yck_major_info$series_name)
#添加是否进口（1为进口）
is_import<-str_extract(config_vdatabase_yck_major_info$series_group_name,'进口')
is_import[-which(is.na(is_import))]<-1
is_import[which(is.na(is_import))]<-0

#品牌、车系配置表
config_vdatabase_yck_major_info<-config_vdatabase_yck_major_info %>% group_by(Initial) %>% mutate(yck_brandid=paste0(Initial,as.integer(factor(brand_name)))) %>%
  group_by(brand_name) %>% mutate(yck_seriesid=paste0(yck_brandid,'S',as.integer(factor(series_name)))) %>% as.data.frame() %>% 
  group_by(brand_name,series_name) %>% mutate(yck_modelid=paste0(yck_seriesid,'M',as.integer(factor(model_id)))) %>% as.data.frame() %>%
  dplyr::select(yck_modelid,model_id,Initial,brandid,yck_brandid,brand_name,series_group_name,series_id,yck_seriesid,series_name,
                model_name,model_price,model_year,auto,liter,discharge_standard,max_reg_year,min_reg_year,car_level,seat_number,hl_configs,
                hl_configc,is_green) %>% mutate(is_import=is_import)

#入库
write.csv(config_vdatabase_yck_major_info,paste0(deep_local,"\\config_vdatabase_yck_major_info.csv",sep=""),row.names = F,fileEncoding = "UTF-8",quote = F)
loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
dbSendQuery(loc_channel,'SET NAMES gbk')
dbSendQuery(loc_channel,"TRUNCATE TABLE config_vdatabase_yck_major_info")
dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE ","'",paste0(deep_local,"/config_vdatabase_yck_major_info.csv",sep=""),"'",
                               " INTO TABLE config_vdatabase_yck_major_info CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;"))
# dbSendQuery(loc_channel,"TRUNCATE TABLE config_vdatabase_yck_brand")
# dbSendQuery(loc_channel,"TRUNCATE TABLE config_vdatabase_yck_series")
# dbSendQuery(loc_channel,"INSERT INTO config_vdatabase_yck_brand SELECT DISTINCT brandid,brand_name,yck_brandid,car_country FROM config_vdatabase_yck_major_info a
#     INNER JOIN config_series_bcountry b ON a.brand_name=b.brand")
# dbSendQuery(loc_channel,"INSERT INTO config_vdatabase_yck_series SELECT DISTINCT series_id,series_group_name,series_name,yck_seriesid,is_import FROM config_vdatabase_yck_major_info")
dbDisconnect(loc_channel)

# #品牌配置表
# config_vdatabase_yck_brand<-config_vdatabase_yck %>% dplyr::select(brandid,brand_name,yck_brandid)%>%unique()
# #config_vdatabase_yck_brand$brand_name<-iconv(config_vdatabase_yck_brand$brand_name,'gbk','utf-8')
# #车系配置表
# config_vdatabase_yck_series<-config_vdatabase_yck %>% dplyr::select(series_id,series_name,yck_seriesid)%>%unique()
# #config_vdatabase_yck_series$series_name<-iconv(config_vdatabase_yck_series$series_name,'gbk','utf-8')
# # loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
# # dbWriteTable(loc_channel,"config_vdatabase_yck_brand",config_vdatabase_yck_brand,append =T,row.names=F)
# # dbWriteTable(loc_channel,"config_vdatabase_yck_series",config_vdatabase_yck_series,append =T,row.names=F)
# # dbDisconnect(loc_channel)
