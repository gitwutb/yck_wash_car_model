###########################################数据宽表analysis_wide_table##################
#清除缓存
rm(list = ls(all=T))
gc()
library(dplyr)
library(stringr)
library(raster)
library(reshape2)
#读取数据
library(RMySQL)
file_dir<-gsub("\\/bat|\\/main.*","",tryCatch(dirname(rstudioapi::getActiveDocumentContext()$path),error=function(e){getwd()}))
che58_city<- read.csv(paste0(file_dir,"/config/config_file/城市牌照.csv",sep=""),header = T,sep = ",")
loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
dbSendQuery(loc_channel,'SET NAMES gbk')
data_new<-Sys.Date()%>%as.character()
input_orig<-dbFetch(dbSendQuery(loc_channel,paste0("SELECT m.car_platform,m.id_data_input,m.id_che300,p.model_year,p.yck_brandid,p.yck_seriesid,p.is_import,p.is_green,p.brand_name brand,p.series_name series,p.model_name,n.color,p.liter,p.auto,p.discharge_standard,p.car_level,n.location,n.regDate,n.quotes,p.model_price,n.mile,",
                                                   " n.state,n.trans_fee,n.transfer,n.annual,n.insure,DATE_FORMAT(n.add_time,'%Y-%m-%d')  add_time,DATE_FORMAT(n.update_time,'%Y-%m-%d') update_time",
                                                   " FROM (SELECT car_platform,id_data_input,id_che300 FROM analysis_match_id_temp where car_platform='che168' AND match_des='right' AND date_add='",data_new,"') m",
                                                   " INNER JOIN spider_www_168 n ON m.id_data_input=n.id",
                                                   " INNER JOIN config_vdatabase_yck_major_info p ON m.id_che300=p.model_id;")),-1)
config_distr<-dbFetch(dbSendQuery(loc_channel,"SELECT DISTINCT regional,b.province,a.key_municipal city FROM config_district a
                                  INNER JOIN config_district_regional b ON a.key_province=b.province;"),-1)
config_distr_all<-dbFetch(dbSendQuery(loc_channel,"SELECT DISTINCT regional,b.province,a.key_municipal city,a.key_county FROM config_district a
                                  INNER JOIN config_district_regional b ON a.key_province=b.province;"),-1)
config_series_bcountry<-dbFetch(dbSendQuery(loc_channel,"SELECT DISTINCT yck_brandid,car_country FROM config_vdatabase_yck_brand"),-1)
dbDisconnect(loc_channel)
######
input_orig$add_time<-as.Date(input_orig$add_time)
input_orig$mile<-round(input_orig$mile/10000,2)
input_orig$quotes<-round(input_orig$quotes/10000,2)
input_orig<-inner_join(input_orig,config_distr,c("location"="city"))
input_orig<-inner_join(input_orig,config_series_bcountry,by='yck_brandid')
user_years<-round((as.Date(input_orig$add_time)-as.Date(input_orig$regDate))/365,2)
input_orig<-data.frame(input_orig,user_years)
if(nrow(input_orig)==0){
  print("无数据")
}else{
  input_orig$regDate<-cut(as.Date(input_orig$regDate),breaks="month")
}
input_orig$user_years<-as.numeric(as.character(input_orig$user_years))
##年检
input_orig$annual<-as.Date(input_orig$add_time)-as.Date(input_orig$annual)
input_orig$annual[which(input_orig$annual>0)]<-1
input_orig$annual[which(input_orig$annual<=0)]<-0
input_orig$annual<-factor(input_orig$annual)
##保险
input_orig$insure<-as.Date(input_orig$add_time)-as.Date(input_orig$insure)
input_orig$insure[which(input_orig$insure>0)]<-1
input_orig$insure[which(input_orig$insure<=0)]<-0
input_orig$insure<-factor(input_orig$insure)
##分区字段(放最后)
input_orig<-data.frame(input_orig,partition_month=format(input_orig$add_time,"%Y%m"))
wutb<-input_orig
###***********************************监控文件**************************#######
if(nrow(input_orig)==0){
  n_platform<-data.frame(platform="",nr="")
}else{
  n_platform<-data.frame(platform=unique(input_orig$car_platform),nr=nrow(input_orig))
}



#########################################################2222rrc###############################
loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
dbSendQuery(loc_channel,'SET NAMES gbk')
input_orig<-dbFetch(dbSendQuery(loc_channel,paste0("SELECT m.car_platform,m.id_data_input,m.id_che300,p.model_year,p.yck_brandid,p.yck_seriesid,p.is_import,p.is_green,p.brand_name brand,p.series_name series,p.model_name,n.color,p.liter,p.auto,p.discharge_standard,p.car_level,n.location,n.regDate,n.quotes,p.model_price,n.mile,",
                                                   " n.state,'1' trans_fee,n.transfer,n.annual,n.high_insure insure,DATE_FORMAT(n.add_time,'%Y-%m-%d')  add_time,DATE_FORMAT(n.update_time,'%Y-%m-%d') update_time,n.address",
                                                   " FROM (SELECT car_platform,id_data_input,id_che300 FROM analysis_match_id_temp where car_platform='rrc' AND match_des='right' AND date_add='",data_new,"') m",
                                                   " INNER JOIN spider_www_renren n ON m.id_data_input=n.id",
                                                   " INNER JOIN config_vdatabase_yck_major_info p ON m.id_che300=p.model_id;")),-1)
##过滤已经处理数据
dbDisconnect(loc_channel)
######
input_orig$add_time<-as.Date(input_orig$add_time)
input_orig$mile<-round(input_orig$mile/10000,2)
input_orig$quotes<-round(input_orig$quotes/10000,2)
###location清洗
location_ls<-str_extract(input_orig$location,paste0(config_distr$city,sep="",collapse = "|"))
location_ls[which(is.na(location_ls))]<-str_extract(input_orig$address[which(is.na(location_ls))],paste0(config_distr$city,sep="",collapse = "|"))
input_orig$location<-location_ls
input_orig<-inner_join(input_orig,config_distr,c("location"="city"))
#
input_orig<-inner_join(input_orig,config_series_bcountry,by='yck_brandid')
user_years<-round((as.Date(input_orig$add_time)-as.Date(input_orig$regDate))/365,2)
input_orig<-data.frame(input_orig,user_years)
if(nrow(input_orig)==0){
  print("无数据")
}else{
  input_orig$regDate<-cut(as.Date(input_orig$regDate),breaks="month")
}
input_orig$user_years<-as.numeric(as.character(input_orig$user_years))
##年检
input_orig$annual<-as.Date(input_orig$add_time)-as.Date(input_orig$annual)
input_orig$annual[which(input_orig$annual>0)]<-1
input_orig$annual[which(input_orig$annual<=0)]<-0
input_orig$annual<-factor(input_orig$annual)
##保险
input_orig$insure<-as.Date(input_orig$add_time)-as.Date(input_orig$insure)
input_orig$insure[which(input_orig$insure>0)]<-1
input_orig$insure[which(input_orig$insure<=0)]<-0
input_orig$insure<-factor(input_orig$insure)
##分区字段(放最后)
input_orig<-data.frame(input_orig,partition_month=format(input_orig$add_time,"%Y%m"))%>%dplyr::select(-address)
wutb<-rbind(wutb,input_orig)
###***********************************监控文件**************************#######
if(nrow(input_orig)==0){
  print("无数据")
}else{
  n_platform<-rbind(n_platform,data.frame(platform=unique(input_orig$car_platform),nr=nrow(input_orig)))
}
#########################################################rrc###############################


#########################################################3优信###############################
loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
dbSendQuery(loc_channel,'SET NAMES gbk')
input_orig<-dbFetch(dbSendQuery(loc_channel,paste0("SELECT m.car_platform,m.id_data_input,m.id_che300,p.model_year,p.yck_brandid,p.yck_seriesid,p.is_import,p.is_green,p.brand_name brand,p.series_name series,p.model_name,n.color,p.liter,p.auto,p.discharge_standard,p.car_level,n.address location,n.regDate,n.quotes,p.model_price,n.mile,",
                                                   " n.state,'1' trans_fee,'' transfer,n.annual,n.insure,DATE_FORMAT(n.add_time,'%Y-%m-%d')  add_time,DATE_FORMAT(n.update_time,'%Y-%m-%d') update_time",
                                                   " FROM (SELECT car_platform,id_data_input,id_che300 FROM analysis_match_id_temp where car_platform='youxin' AND match_des='right' AND date_add='",data_new,"') m",
                                                   " INNER JOIN spider_www_xin n ON m.id_data_input=n.id",
                                                   " INNER JOIN config_vdatabase_yck_major_info p ON m.id_che300=p.model_id;")),-1)
##过滤已经处理数据
dbDisconnect(loc_channel)
######
input_orig$add_time<-as.Date(input_orig$add_time)
input_orig$mile<-round(input_orig$mile/10000,2)
input_orig$quotes<-round(input_orig$quotes/10000,2)
###location清洗
location_ls<-str_extract(input_orig$location,paste0(config_distr$city,sep="",collapse = "|"))
location_ls1<-data.frame(key_county=str_extract(input_orig$location[which(is.na(location_ls))],paste0(config_distr_all$key_county,sep="",collapse = "|")))
location_ls1$key_county<-as.character(location_ls1$key_county)
location_ls[which(is.na(location_ls))]<-as.character(inner_join(config_distr_all,location_ls1,by="key_county")$city)
input_orig$location<-location_ls
input_orig<-inner_join(input_orig,config_distr,c("location"="city"))
#
input_orig<-inner_join(input_orig,config_series_bcountry,by='yck_brandid')
user_years<-round((as.Date(input_orig$add_time)-as.Date(input_orig$regDate))/365,2)
input_orig<-data.frame(input_orig,user_years)
if(nrow(input_orig)==0){
  print("无数据")
}else{
  input_orig$regDate<-cut(as.Date(input_orig$regDate),breaks="month")
}
input_orig$user_years<-as.numeric(as.character(input_orig$user_years))
##年检
input_orig$annual<-as.Date(input_orig$add_time)-as.Date(input_orig$annual)
input_orig$annual[which(input_orig$annual>0)]<-1
input_orig$annual[which(input_orig$annual<=0)]<-0
input_orig$annual<-factor(input_orig$annual)
##保险
input_orig$insure<-as.Date(input_orig$add_time)-as.Date(input_orig$insure)
input_orig$insure[which(input_orig$insure>0)]<-1
input_orig$insure[which(input_orig$insure<=0)]<-0
input_orig$insure<-factor(input_orig$insure)
##分区字段(放最后)
input_orig<-data.frame(input_orig,partition_month=format(input_orig$add_time,"%Y%m"))
wutb<-rbind(wutb,input_orig)
###***********************************监控文件**************************#######
if(nrow(input_orig)==0){
  print("无数据")
}else{
  n_platform<-rbind(n_platform,data.frame(platform=unique(input_orig$car_platform),nr=nrow(input_orig)))
}
#########################################################rrc###############################

#####################################################44444车速拍###############################
loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
dbSendQuery(loc_channel,'SET NAMES gbk')
input_orig<-dbFetch(dbSendQuery(loc_channel,paste0("SELECT m.car_platform,m.id_data_input,m.id_che300,p.model_year,p.yck_brandid,p.yck_seriesid,p.is_import,p.is_green,p.brand_name brand,p.series_name series,p.model_name,n.color,p.liter,p.auto,p.discharge_standard,p.car_level,n.location,n.regDate,n.quotes,p.model_price,n.mile,",
                                                   " n.state,'0' trans_fee,n.transfer,n.annual,n.high_insure insure,DATE_FORMAT(n.add_time,'%Y-%m-%d')  add_time,DATE_FORMAT(n.update_time,'%Y-%m-%d') update_time,n.address",
                                                   " FROM (SELECT car_platform,id_data_input,id_che300 FROM analysis_match_id_temp where car_platform='csp' AND match_des='right' AND date_add='",data_new,"') m",
                                                   " INNER JOIN spider_www_chesupai n ON m.id_data_input=n.id",
                                                   " INNER JOIN config_vdatabase_yck_major_info p ON m.id_che300=p.model_id;")),-1)
##过滤已经处理数据
dbDisconnect(loc_channel)
######
input_orig$add_time<-as.Date(input_orig$add_time)
input_orig$mile<-round(input_orig$mile/10000,2)
input_orig$quotes<-round(input_orig$quotes/10000,2)
###location清洗&nbsp
input_orig$location<-gsub("&nbsp","",input_orig$location)
location_ls<-str_extract(input_orig$location,paste0(config_distr$city,sep="",collapse = "|"))
location_ls[which(is.na(location_ls))]<-str_extract(input_orig$address[which(is.na(location_ls))],paste0(config_distr$city,sep="",collapse = "|"))
location_ls1<-data.frame(key_county=str_extract(input_orig$location[which(is.na(location_ls))],paste0(config_distr_all$key_county,sep="",collapse = "|")))
location_ls1$key_county<-as.character(location_ls1$key_county)
location_ls[which(is.na(location_ls))]<-as.character(right_join(config_distr_all,location_ls1,by="key_county")$city)
input_orig$location<-location_ls
input_orig<-inner_join(input_orig,config_distr,c("location"="city"))
#
input_orig<-inner_join(input_orig,config_series_bcountry,by='yck_brandid')
user_years<-round((as.Date(input_orig$add_time)-as.Date(input_orig$regDate))/365,2)
input_orig<-data.frame(input_orig,user_years)
if(nrow(input_orig)==0){
  print("无数据")
}else{
  input_orig$regDate<-cut(as.Date(input_orig$regDate),breaks="month")
}
input_orig$user_years<-as.numeric(as.character(input_orig$user_years))
##年检
input_orig$annual<-as.Date(input_orig$add_time)-as.Date(input_orig$annual)
input_orig$annual[which(input_orig$annual>0)]<-1
input_orig$annual[which(input_orig$annual<=0)]<-0
input_orig$annual<-factor(input_orig$annual)
##保险
input_orig$insure<-as.Date(input_orig$add_time)-as.Date(input_orig$insure)
input_orig$insure[which(input_orig$insure>0)]<-1
input_orig$insure[which(input_orig$insure<=0)]<-0
input_orig$insure<-factor(input_orig$insure)
##分区字段(放最后)
input_orig<-data.frame(input_orig,partition_month=format(input_orig$add_time,"%Y%m"))%>%dplyr::select(-address)
wutb<-rbind(wutb,input_orig)
###***********************************监控文件**************************#######
if(nrow(input_orig)==0){
  print("无数据")
}else{
  n_platform<-rbind(n_platform,data.frame(platform=unique(input_orig$car_platform),nr=nrow(input_orig)))
}
#########################################################车速拍#######################################


#####################################################55555瓜子###############################
loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
dbSendQuery(loc_channel,'SET NAMES gbk')
input_orig<-dbFetch(dbSendQuery(loc_channel,paste0("SELECT m.car_platform,m.id_data_input,m.id_che300,p.model_year,p.yck_brandid,p.yck_seriesid,p.is_import,p.is_green,p.brand_name brand,p.series_name series,p.model_name,'-' color,p.liter,p.auto,p.discharge_standard,p.car_level,n.location,n.regDate,n.quotes,p.model_price,n.mile,",
                                                   " n.state,'0' trans_fee,n.transfer,n.annual,n.high_insure insure,DATE_FORMAT(n.add_time,'%Y-%m-%d')  add_time,DATE_FORMAT(n.update_time,'%Y-%m-%d') update_time,n.address",
                                                   " FROM (SELECT car_platform,id_data_input,id_che300 FROM analysis_match_id_temp where car_platform='guazi' AND match_des='right' AND date_add='",data_new,"') m",
                                                   " INNER JOIN spider_www_guazi n ON m.id_data_input=n.id",
                                                   " INNER JOIN config_vdatabase_yck_major_info p ON m.id_che300=p.model_id;")),-1)
##过滤已经处理数据
dbDisconnect(loc_channel)
######
input_orig$add_time<-as.Date(input_orig$add_time)
input_orig$mile<-round(input_orig$mile/10000,2)
input_orig$quotes<-round(input_orig$quotes/10000,2)
#######location清洗&nbsp
input_orig$location<-gsub("襄樊","襄阳",input_orig$location)
input_orig$location<-gsub("杨凌","咸阳",input_orig$location)
input_orig$location<-gsub("农垦系统","哈尔滨",input_orig$location)
location_ls<-str_extract(input_orig$location,paste0(config_distr$city,sep="",collapse = "|"))
location_ls[which(is.na(location_ls))]<-str_extract(input_orig$address[which(is.na(location_ls))],paste0(config_distr$city,sep="",collapse = "|"))
location_ls1<-data.frame(key_county=str_extract(input_orig$location[which(is.na(location_ls))],paste0(config_distr_all$key_county,sep="",collapse = "|")))
location_ls1$key_county<-as.character(location_ls1$key_county)
location_ls[which(is.na(location_ls))]<-as.character(right_join(config_distr_all,location_ls1,by="key_county")$city)
#剩余各区存在重名
location_ls2<-data.frame(key_county=input_orig$address[which(is.na(location_ls))])
location_ls2$key_county<-as.character(location_ls2$key_county)
laji<-inner_join(location_ls2,config_distr_all,by="key_county")
laji1<-as.data.frame(table(inner_join(location_ls2,config_distr_all,by="key_county")[,c("city","key_county")]%>%unique()%>%.[2]))%>%filter(Freq==1)
##########2018/05/07修改#############
if(nrow(laji1)>0){
  laji<-inner_join(laji,laji1,c("key_county"="Var1"))[,c("city","key_county")]%>%unique()
  location_ls[which(is.na(location_ls))]<-as.character(left_join(location_ls2,laji,by="key_county")[,"city"])
  location_ls[which(is.na(location_ls))]<-""
}else{
  location_ls[which(is.na(location_ls))]<-""
}

#最终
input_orig$location<-location_ls
input_orig<-inner_join(input_orig,config_distr,c("location"="city"))
rm(laji,laji1,location_ls2)
##########
input_orig<-inner_join(input_orig,config_series_bcountry,by='yck_brandid')
user_years<-round((as.Date(input_orig$add_time)-as.Date(input_orig$regDate))/365,2)
input_orig<-data.frame(input_orig,user_years)
if(nrow(input_orig)==0){
  print("无数据")
}else{
  input_orig$regDate<-cut(as.Date(input_orig$regDate),breaks="month")
}
input_orig$user_years<-as.numeric(as.character(input_orig$user_years))
##年检
input_orig$annual<-as.Date(input_orig$add_time)-as.Date(input_orig$annual)
input_orig$annual[which(input_orig$annual>0)]<-1
input_orig$annual[which(input_orig$annual<=0)]<-0
input_orig$annual<-factor(input_orig$annual)
##保险
input_orig$insure<-as.Date(input_orig$add_time)-as.Date(input_orig$insure)
input_orig$insure[which(input_orig$insure>0)]<-1
input_orig$insure[which(input_orig$insure<=0)]<-0
input_orig$insure<-factor(input_orig$insure)
##分区字段(放最后)
input_orig<-data.frame(input_orig,partition_month=format(input_orig$add_time,"%Y%m"))%>%dplyr::select(-address)
wutb<-rbind(wutb,input_orig)
###***********************************监控文件**************************#######
if(nrow(input_orig)==0){
  print("无数据")
}else{
  n_platform<-rbind(n_platform,data.frame(platform=unique(input_orig$car_platform),nr=nrow(input_orig)))
}
#########################################################车速拍#######################################

#####################################################6666车58###############################
loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
dbSendQuery(loc_channel,'SET NAMES gbk')


input_orig<-dbFetch(dbSendQuery(loc_channel,paste0("SELECT m.car_platform,m.id_data_input,m.id_che300,p.model_year,p.yck_brandid,p.yck_seriesid,p.is_import,p.is_green,p.brand_name brand,p.series_name series,p.model_name,n.color,p.liter,p.auto,p.discharge_standard,p.car_level,n.url location,n.regDate,n.quotes,p.model_price,n.mile,",
                                                   " '' state,n.trans_fee,'' transfer,n.annual,n.high_insure insure,DATE_FORMAT(n.add_time,'%Y-%m-%d')  add_time,DATE_FORMAT(n.update_time,'%Y-%m-%d') update_time",
                                                   " FROM (SELECT car_platform,id_data_input,id_che300 FROM analysis_match_id_temp where car_platform='che58' AND match_des='right' AND date_add='",data_new,"') m",
                                                   " INNER JOIN spider_www_58 n ON m.id_data_input=n.id",
                                                   " INNER JOIN config_vdatabase_yck_major_info p ON m.id_che300=p.model_id;")),-1)
dbDisconnect(loc_channel)
######
input_orig$add_time<-as.Date(input_orig$add_time)
input_orig$mile<-round(input_orig$mile/10000,2)
input_orig$quotes<-round(input_orig$quotes/10000,2)
#######location清洗&nbsp
che58_city$che58_name<-as.character(che58_city$che58_name)
input_orig$location<-gsub("http://qh.58.*","琼海",input_orig$location)
input_orig$location<-gsub("http://|.58.com.*","",input_orig$location)
input_orig<-inner_join(input_orig,che58_city,c("location"="che58_name"))
input_orig$location<-input_orig$city
##########
input_orig<-inner_join(input_orig,config_series_bcountry,by='yck_brandid')
user_years<-round((as.Date(input_orig$add_time)-as.Date(input_orig$regDate))/365,2)
input_orig<-data.frame(input_orig,user_years)
if(nrow(input_orig)==0){
  print("无数据")
}else{
  input_orig$regDate<-cut(as.Date(input_orig$regDate),breaks="month")
}
input_orig$user_years<-as.numeric(as.character(input_orig$user_years))
##年检
input_orig$annual[grep("年[1-9]月",input_orig$annual)]<-gsub("年","年0",input_orig$annual[grep("年[1-9]月",input_orig$annual)])
input_orig$annual<-gsub("车主未填写|年","",input_orig$annual)
input_orig$annual<-gsub("月","01",input_orig$annual)
input_orig$annual<-gsub("过保","19800101",input_orig$annual)
input_orig$annual<-as.Date(input_orig$add_time)-as.Date(input_orig$annual,format = "%Y%m%d")
input_orig$annual[which(input_orig$annual>0)]<-1
input_orig$annual[which(input_orig$annual<=0)]<-0
input_orig$annual<-factor(input_orig$annual)
##保险
##年检
input_orig$insure[grep("年[1-9]月",input_orig$insure)]<-gsub("年","年0",input_orig$insure[grep("年[1-9]月",input_orig$insure)])
input_orig$insure<-gsub("车主未填写|年","",input_orig$insure)
input_orig$insure<-gsub("月","01",input_orig$insure)
input_orig$insure<-gsub("过保","19800101",input_orig$insure)
input_orig$insure<-as.Date(input_orig$add_time)-as.Date(input_orig$insure,format = "%Y%m%d")
input_orig$insure[which(input_orig$insure>0)]<-1
input_orig$insure[which(input_orig$insure<=0)]<-0
input_orig$insure<-factor(input_orig$insure)
##分区字段(放最后)
input_orig<-data.frame(input_orig,partition_month=format(input_orig$add_time,"%Y%m"))%>%dplyr::select(-city)
wutb<-rbind(wutb,input_orig)
###***********************************监控文件**************************#######
if(nrow(input_orig)==0){
  print("无数据")
}else{
  n_platform<-rbind(n_platform,data.frame(platform=unique(input_orig$car_platform),nr=nrow(input_orig)))
}
####################################################################################################


#####################################################77777易车###############################
loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
dbSendQuery(loc_channel,'SET NAMES gbk')


input_orig<-dbFetch(dbSendQuery(loc_channel,paste0("SELECT m.car_platform,m.id_data_input,m.id_che300,p.model_year,p.yck_brandid,p.yck_seriesid,p.is_import,p.is_green,p.brand_name brand,p.series_name series,p.model_name,'' color,p.liter,p.auto,p.discharge_standard,p.car_level,n.location,n.regDate,n.quotes,p.model_price,n.mile,",
                                                   " '' state,n.trans_fee,'' transfer,n.annual,insure,DATE_FORMAT(n.add_time,'%Y-%m-%d')  add_time,DATE_FORMAT(n.update_time,'%Y-%m-%d') update_time",
                                                   " FROM (SELECT car_platform,id_data_input,id_che300 FROM analysis_match_id_temp where car_platform='yiche' AND match_des='right' AND date_add='",data_new,"') m",
                                                   " INNER JOIN spider_www_yiche n ON m.id_data_input=n.id",
                                                   " INNER JOIN config_vdatabase_yck_major_info p ON m.id_che300=p.model_id;")),-1)
dbDisconnect(loc_channel)
######
input_orig$add_time<-as.Date(input_orig$add_time)
input_orig$mile<-round(input_orig$mile/10000,2)
input_orig$quotes<-round(input_orig$quotes/10000,2)
#######location清洗&nbsp
input_orig$location<-gsub("巢湖","合肥",input_orig$location)
location_ls<-str_extract(input_orig$location,paste0(config_distr$city,sep="",collapse = "|"))
location_ls[which(is.na(location_ls))]<-str_extract(input_orig$location[which(is.na(location_ls))],paste0(unique(config_distr$province),sep="",collapse = "|"))
location_ls<-data.frame(city=location_ls)
location_ls<-left_join(location_ls,config_distr,c("city"="city"))
location_ls$province[which(is.na(location_ls$province))]<-location_ls$city[which(is.na(location_ls$province))]
location_ls$regional[which(is.na(location_ls$regional))]<-
  as.character(left_join(location_ls[which(is.na(location_ls$regional)),],unique(config_distr[,1:2]),by="province")$regional.y)
input_orig$location<-location_ls$city
input_orig<-data.frame(input_orig,regional=location_ls$regional,province=location_ls$province)
if(length(which(is.na(location_ls$regional)))>0){input_orig<-input_orig[-which(is.na(location_ls$regional)),]}
##########
input_orig<-inner_join(input_orig,config_series_bcountry,by='yck_brandid')
user_years<-round((as.Date(input_orig$add_time)-as.Date(input_orig$regDate))/365,2)
input_orig<-data.frame(input_orig,user_years)
if(nrow(input_orig)==0){
  print("无数据")
}else{
  input_orig$regDate<-cut(as.Date(input_orig$regDate),breaks="month")
}
input_orig$user_years<-as.numeric(as.character(input_orig$user_years))
##年检
input_orig$annual<-gsub("车主未填写|年|- -","",input_orig$annual)
input_orig$annual<-gsub("月","01",input_orig$annual)
input_orig$annual<-gsub("过保|已过期|已到期","19800101",input_orig$annual)
input_orig$annual<-as.Date(input_orig$add_time)-as.Date(input_orig$annual,format = "%Y%m%d")
input_orig$annual[which(input_orig$annual>0)]<-1
input_orig$annual[which(input_orig$annual<=0)]<-0
input_orig$annual<-factor(input_orig$annual)
##保险
input_orig$insure<-gsub("车主未填写|年|- -","",input_orig$insure)
input_orig$insure<-gsub("月","01",input_orig$insure)
input_orig$insure<-gsub("过保|已过期|已到期","19800101",input_orig$insure)
input_orig$insure<-as.Date(input_orig$add_time)-as.Date(input_orig$insure,format = "%Y%m%d")
input_orig$insure[which(input_orig$insure>0)]<-1
input_orig$insure[which(input_orig$insure<=0)]<-0
input_orig$insure<-factor(input_orig$insure)
##分区字段(放最后)
input_orig<-data.frame(input_orig,partition_month=format(input_orig$add_time,"%Y%m"))
wutb<-rbind(wutb,input_orig)
###***********************************监控文件**************************#######
if(nrow(input_orig)==0){
  print("无数据")
}else{
  n_platform<-rbind(n_platform,data.frame(platform=unique(input_orig$car_platform),nr=nrow(input_orig)))
}
################################################77777####################################################


#####################################################88888车车置宝###############################
loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
dbSendQuery(loc_channel,'SET NAMES gbk')


input_orig<-dbFetch(dbSendQuery(loc_channel,paste0("SELECT m.car_platform,m.id_data_input,m.id_che300,p.model_year,p.yck_brandid,p.yck_seriesid,p.is_import,p.is_green,p.brand_name brand,p.series_name series,p.model_name,'' color,p.liter,p.auto,p.discharge_standard,p.car_level,n.location,n.regDate,n.bid_price quotes,p.model_price,n.mile,",
                                                   " '' state,'' trans_fee,n.transfer,n.annual,'' insure,DATE_FORMAT(n.add_time,'%Y-%m-%d')  add_time,DATE_FORMAT(n.update_time,'%Y-%m-%d') update_time",
                                                   " FROM (SELECT car_platform,id_data_input,id_che300 FROM analysis_match_id_temp where car_platform='czb' AND match_des='right' AND date_add='",data_new,"') m",
                                                   " INNER JOIN spider_www_chezhibao n ON m.id_data_input=n.id",
                                                   " INNER JOIN config_vdatabase_yck_major_info p ON m.id_che300=p.model_id;")),-1)
##过滤已经处理数据
dbDisconnect(loc_channel)
######
input_orig$add_time<-as.Date(input_orig$add_time)
input_orig$mile<-round(input_orig$mile/10000,2)
input_orig$quotes<-round(input_orig$quotes/10000,2)

#######location清洗&nbsp
input_orig$location<-gsub("襄樊","襄阳",input_orig$location)
input_orig$location<-gsub("杨凌|杨陵","咸阳",input_orig$location)
input_orig$location<-gsub("农垦系统","哈尔滨",input_orig$location)
input_orig$location<-gsub("湖北","武汉",input_orig$location)
location_ls<-str_extract(input_orig$location,paste0(config_distr$city,sep="",collapse = "|"))
#通过区县
location_ls1<-data.frame(key_county=str_extract(input_orig$location[which(is.na(location_ls))],paste0(config_distr_all$key_county,sep="",collapse = "|")))
location_ls1$key_county<-as.character(location_ls1$key_county)
location_ls[which(is.na(location_ls))]<-as.character(right_join(config_distr_all,location_ls1,by="key_county")$city)
#最终
input_orig$location<-location_ls
input_orig<-inner_join(input_orig,config_distr,c("location"="city"))
##########
input_orig<-inner_join(input_orig,config_series_bcountry,by='yck_brandid')
user_years<-round((as.Date(input_orig$add_time)-as.Date(input_orig$regDate))/365,2)
input_orig<-data.frame(input_orig,user_years)
if(nrow(input_orig)==0){
  print("无数据")
}else{
  input_orig$regDate<-cut(as.Date(input_orig$regDate),breaks="month")
}
input_orig$user_years<-as.numeric(as.character(input_orig$user_years))
##年检
input_orig$annual<-as.Date(input_orig$add_time)-as.Date(input_orig$annual)
input_orig$annual[which(input_orig$annual>0)]<-1
input_orig$annual[which(input_orig$annual<=0)]<-0
input_orig$annual<-factor(input_orig$annual)
##保险(无)
##转手(大于等于1次)
input_orig$transfer<-gsub("无","0",input_orig$transfer)
input_orig$transfer<-gsub(".*-.*","1",input_orig$transfer)
##分区字段(放最后)
input_orig<-data.frame(input_orig,partition_month=format(input_orig$add_time,"%Y%m"))
wutb<-rbind(wutb,input_orig)
###***********************************监控文件**************************#######
if(nrow(input_orig)==0){
  print("无数据")
}else{
  n_platform<-rbind(n_platform,data.frame(platform=unique(input_orig$car_platform),nr=nrow(input_orig)))
}
################################################88888车车置宝####################################################

# #####################################################9999搜车###############################
# loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
# dbSendQuery(loc_channel,'SET NAMES gbk')
# 
# input_orig<-dbFetch(dbSendQuery(loc_channel,paste0("SELECT m.car_platform,m.id_data_input,m.id_che300,p.model_year,p.yck_brandid,p.yck_seriesid,p.is_import,p.is_green,p.brand_name brand,p.series_name series,p.model_name,n.color,p.liter,p.auto,p.discharge_standard,p.car_level,n.location,n.regDate,n.quotes,p.model_price,n.mile,",
#                                                    " '' state,'' trans_fee,'' transfer,'' annual,'' insure,DATE_FORMAT(n.add_time,'%Y-%m-%d')  add_time,DATE_FORMAT(n.update_time,'%Y-%m-%d') update_time",
#                                                    " FROM (SELECT car_platform,id_data_input,id_che300 FROM analysis_match_id_temp where car_platform='souche' AND match_des='right' AND date_add='",data_new,"') m",
#                                                    " INNER JOIN spider_www_souche n ON m.id_data_input=n.id",
#                                                    " INNER JOIN config_vdatabase_yck_major_info p ON m.id_che300=p.model_id;")),-1)
# ##过滤已经处理数据
# dbDisconnect(loc_channel)
# ######
# input_orig$add_time<-as.Date(input_orig$add_time)
# input_orig$mile<-round(input_orig$mile/10000,2)
# input_orig$quotes<-round(input_orig$quotes/10000,2)
# 
# #######location清洗&nbsp
# input_orig$location<-gsub("襄樊","襄阳",input_orig$location)
# input_orig$location<-gsub("杨凌|杨陵","咸阳",input_orig$location)
# input_orig$location<-gsub("农垦系统","哈尔滨",input_orig$location)
# input_orig$location<-gsub("湖北","武汉",input_orig$location)
# location_ls<-str_extract(input_orig$location,paste0(config_distr$city,sep="",collapse = "|"))
# #通过区县
# location_ls1<-data.frame(key_county=str_extract(input_orig$location[which(is.na(location_ls))],paste0(config_distr_all$key_county,sep="",collapse = "|")))
# location_ls1$key_county<-as.character(location_ls1$key_county)
# location_ls[which(is.na(location_ls))]<-as.character(right_join(config_distr_all,location_ls1,by="key_county")$city)
# #最终
# input_orig$location<-location_ls
# input_orig<-inner_join(input_orig,config_distr,c("location"="city"))
# ##########
# input_orig<-inner_join(input_orig,config_series_bcountry,by='yck_brandid')
# user_years<-round((as.Date(input_orig$add_time)-as.Date(input_orig$regDate))/365,2)
# input_orig<-data.frame(input_orig,user_years)
# if(nrow(input_orig)==0){
#   print("无数据")
# }else{
#   input_orig$regDate<-cut(as.Date(input_orig$regDate),breaks="month")
# }
# input_orig$user_years<-as.numeric(as.character(input_orig$user_years))
# ##年检(无)
# ##保险(无)
# ##转手(大于等于1次)
# input_orig$transfer<-gsub("无","0",input_orig$transfer)
# input_orig$transfer<-gsub(".*-.*","1",input_orig$transfer)
# ##分区字段(放最后)
# input_orig<-data.frame(input_orig,partition_month=format(input_orig$add_time,"%Y%m"))
# wutb<-rbind(wutb,input_orig)
# ###***********************************监控文件**************************#######
# if(nrow(input_orig)==0){
#   print("无数据")
# }else{
#   n_platform<-rbind(n_platform,data.frame(platform=unique(input_orig$car_platform),nr=nrow(input_orig)))
# }
# ################################################9999搜车####################################################

###清洗为NA
wutb$annual[which(is.na(wutb$annual))]<-''
wutb$transfer[which(is.na(wutb$transfer))]<-''
wutb$insure[which(is.na(wutb$insure))]<-''
wutb$state[which(is.na(wutb$state))]<-''
wutb$trans_fee[which(is.na(wutb$trans_fee))]<-''
wutb$transfer<-gsub('.*数据|NA','',wutb$transfer)
wutb$annual<-gsub('NA','',wutb$annual)
wutb$insure<-gsub('NA','',wutb$insure)
wutb$state<-gsub('NA','',wutb$state)
wutb$trans_fee<-gsub('NA','',wutb$trans_fee)

#清洗color
wutb$color<-gsub("――|-|无数据|null|[0-9]","",wutb$color)
wutb$color<-gsub("其他","其它",wutb$color)
wutb$color<-gsub("色","",wutb$color)
wutb$color<-gsub("浅|深|象牙|冰川","",wutb$color)
wutb<-data.frame(wutb,date_add=format(Sys.time(),'%Y-%m-%d'))
write.csv(wutb,paste0(file_dir,"/file/output_final/analysis_wide_table.csv",sep=""),
          row.names = F,fileEncoding = "UTF-8",quote = F)

##日志文件
n_platform<-data.frame(n_platform,date=Sys.time())
write.table(n_platform,paste0(file_dir,"/file/output_final/analysis_wide_table.txt",sep=""),row.names = F,append = T)
