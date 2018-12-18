#SELECT id car_id,car model_name,emission discharge_standard,gearbox car_auto FROM che58 a
#  where CONCAT(DATE_FORMAT(a.timestamp,'%Y'),DATE_FORMAT(a.timestamp,'%m'),DATE_FORMAT(a.timestamp,'%d'))<'20170926';
#清除缓存
rm(list = ls(all=T))
gc()
library(dplyr)
library(stringr)
library(raster)
#help(package="dplyr")
#读取数据
library(RMySQL)
deep_local<-c("E:\\Work_table\\gitwutb\\git_project\\")
loc_channel<-dbConnect(MySQL(),user = "root",host="192.168.0.111",password= "000000",dbname="yck-data-center")
dbSendQuery(loc_channel,'SET NAMES gbk')
table.name<-dbListTables(loc_channel)
# field.name<-dbListFields(loc_channel,"")
yck_rrc<-dbFetch(dbSendQuery(loc_channel,"SELECT id car_id,brand brand_name,series series_name,model model_name,tax_price model_price,emission discharge_standard,displacement liter FROM spider_www_renren a 
                             WHERE a.id>(SELECT MAX(id_data_input) FROM analysis_match_id WHERE car_platform in ('rrc'));"),-1)
rm_series_rule<-dbFetch(dbSendQuery(loc_channel,"SELECT * FROM config_reg_series_rule;"),-1)
che300<-dbFetch(dbSendQuery(loc_channel,"SELECT * FROM analysis_che300_cofig_info;"),-1)
dbDisconnect(loc_channel)
rm_rule<- read.csv(paste0(deep_local,"yck_wash_car_model\\config\\config_file\\reg_rule.csv",sep=""),header = T,sep = ",")
source(paste0(deep_local,"yck_wash_car_model\\config\\config_fun\\fun_stopWords.R",sep=""),echo=TRUE,encoding="utf-8")
source(paste0(deep_local,"yck_wash_car_model\\config\\config_fun\\fun_normalization.R",sep=""),echo=TRUE,encoding="utf-8")


######################------第一部分：得到车的品牌及车系-------#################
##临时车名
input_test1<-yck_rrc
input_test1$brand_name<-gsub(" ","",input_test1$brand_name)
input_test1$series_name<-gsub(" ","",input_test1$series_name)
input_test1$model_name<-gsub(" ","",input_test1$model_name)
#input_test1清洗
input_test1$brand_name<-fun_normalization(input_test1$brand_name)
input_test1$series_name<-fun_normalization(input_test1$series_name)
input_test1$model_name<-fun_normalization(input_test1$model_name)
input_test1$series_name<-gsub("MINI |SMART |H-1|VELOSTER|劳恩斯-|别克|\\·","",input_test1$series_name)
input_test1$series_name[grep("劲炫ASX",input_test1$model_name)]<-gsub("劲炫ASX|劲炫","劲炫ASX",input_test1$series_name[grep("劲炫ASX",input_test1$model_name)])
input_test1$car_id<-as.integer(input_test1$car_id)
rm_series_rule$qx_series_all<-as.character(rm_series_rule$qx_series_all)
rm_series_rule$series<-as.character(rm_series_rule$series)
brand_name<-str_extract(input_test1$brand_name,c(str_c(rm_series_rule$rule_name,sep="",collapse = "|")))
brand_name[which(is.na(brand_name))]<-input_test1$brand_name[which(is.na(brand_name))]
linshi_series<-c(str_c(rm_series_rule$rule_series,sep="",collapse = "|"))
series_name<-str_extract(input_test1$series_name,gsub(" ","",linshi_series))
series_name[which(is.na(series_name))]<-input_test1$series_name[which(is.na(series_name))]
linshi<-str_extract(input_test1$model_name,"A佳|N3佳|N3")
linshi[which(is.na(linshi))]<-""
linshi[-grep("夏利",series_name)]<-""
series_name<-paste(series_name,linshi,sep = "")
series_name[grep("赛欧3",input_test1$model_name)]<-str_extract(input_test1$model_name[grep("赛欧3",input_test1$model_name)],"赛欧3")
####------重新将新的brand及series替换------##
input_test1$brand_name<-brand_name
input_test1$series_name<-series_name
input_test1$brand_name[grep("奔奔MINI",input_test1$model_name)]<-"长安轿车"
input_test1$brand_name[grep("瑞风S2MINI",input_test1$model_name)]<-"江淮"
input_test1$brand_name[grep("新豹MINI",input_test1$model_name)]<-"长安商用"
input_test1$series_name[grep("COUNTRYMAN",input_test1$model_name)]<-"MINI"

seriesFun<-function(i){
  sub(input_test1$brand_name[i],"",input_test1$series_name[i])
}
qx_series_des<-unlist(lapply(1:length(input_test1$series_name),seriesFun))
qx_series_des<-trim(qx_series_des)
qx_series_all<-paste(input_test1$brand_name,qx_series_des,sep = "")
qx_series_des[which(nchar(qx_series_des)==0)]<-input_test1$brand_name[which(nchar(qx_series_des)==0)]
input_test1<-data.frame(input_test1,qx_series_all)
##############--------匹配全称------#########
input_test1$qx_series_all<-as.character(input_test1$qx_series_all)
#匹配到全称
a1<-inner_join(input_test1,rm_series_rule,by="qx_series_all")%>%
  dplyr::select(car_id,id)
a2<-data.frame(car_id=as.integer(setdiff(input_test1$car_id,a1$car_id)))
input_test1<-inner_join(input_test1,a2,by="car_id")%>%dplyr::select(-qx_series_all)
####################################第二轮：系列名匹配######################
a2<-inner_join(input_test1,rm_series_rule,c("series_name"="series"))%>%
  dplyr::select(car_id,id)
a3<-data.frame(car_id=as.integer(setdiff(input_test1$car_id,a2$car_id)))
input_test1<-inner_join(input_test1,a3,by="car_id")
data_input_0<-rbind(a1,a2)
data_input_0<-inner_join(data_input_0,yck_rrc,by="car_id")
data_input_0<-inner_join(data_input_0,rm_series_rule,by="id")%>%
  dplyr::select(car_id,brand_name=name,series_name=series,model_name,model_price,discharge_standard,liter)
data_input_0<-rbind(data_input_0,input_test1)
linshi<-str_extract(data_input_0$model_name,"进口")
linshi[which(is.na(linshi))]<-""
linshi<-gsub("进口","-进口",linshi)
data_input_0$series_name<-paste(data_input_0$series_name,linshi,sep = "")
data_input_0$model_name<-toupper(data_input_0$model_name)
data_input_0<-data.frame(data_input_0,auto="")
#---准确性accurate
accurate<-c(nrow(a3),nrow(yck_rrc))
rm(a1,a2,a3,input_test1,qx_series_all,qx_series_des,linshi,linshi_series,brand_name)
gc()


######################------第二部分：清洗model_name-------#################
#-----数据转换名称------
data_input<-data_input_0
qx_name<-toupper(data_input$model_name)
qx_name<-trim(qx_name)
qx_name<-gsub(" +"," ",qx_name)
car_model_name<-data_input$model_name

##-------------part1：得到年款及指导价----------------#
qx_name<-gsub("2012年","2012款",qx_name)
qx_name<-gsub("-2011 款","2011款 ",qx_name)
car_year<-str_extract(qx_name,c(str_c(1990:2030,"款",sep="",collapse='|')))
car_year<-gsub("款","",car_year)
loc_year<-str_locate(qx_name,c(str_c(1990:2030,"款",sep="",collapse='|')))
car_price<-round(data_input$model_price/10000,2)
qx_name<-str_sub(qx_name,loc_year[,2]+1)
#清除年年款
qx_name<-gsub(c(str_c(1990:2030,"款",sep="",collapse='|')),"",qx_name)
qx_name<-gsub("\\（","(",qx_name)
qx_name<-gsub("\\）",")",qx_name)
qx_name<-gsub("\\(进口\\)","",qx_name)
qx_name<-gsub("\\(海外\\)|赛欧3","",qx_name)
qx_name<-gsub("POWER DAILY","宝迪",qx_name)
qx_name<-gsub("III","Ⅲ",qx_name)
qx_name<-gsub("II","Ⅱ",qx_name)
qx_name<-gsub("—|－","-",qx_name)
qx_name<-gsub("\\·|\\?|(-|)JL465Q","",qx_name)
qx_name<-gsub("ONE\\+","ONE佳",qx_name)
qx_name<-gsub("劲能版\\+","劲能版佳",qx_name)
qx_name<-gsub("选装(包|)","佳",qx_name)
qx_name<-gsub("格锐","格越",qx_name)
qx_name<-gsub("Axela昂克赛拉|Axela","昂克赛拉",qx_name)
qx_name<-gsub("2\\/3|2\\+2\\+3|2\\+3\\+2","",qx_name)

##car_name---------得到品牌名-系列名---------qx<-data.frame(qx_name)
car_name<-toupper(data_input$brand_name)
car_series1<-toupper(data_input$series_name)

##2、清洗系列
seriesFun<-function(i){
  gsub(car_series1[i],"",qx_name[i])
}
qx_name<-unlist(lapply(1:length(car_name),seriesFun))



##--------------------part2:停用词清洗--------------------
####################################################
###-------词语描述归一-----
output_data<-fun_stopWords(data_input,qx_name)
#------------------数据保存----------------
qx_rrc<-data.frame(X=data_input_0$car_id,output_data)
#清洗多余空格
qx_rrc<-trim(qx_rrc)
qx_rrc$car_model_name<-gsub(" +"," ",qx_rrc$car_model_name)
qx_rrc<-sapply(qx_rrc,as.character)
for (i in 1:dim(qx_rrc)[2]) {
  qx_rrc[,i][which(is.na(qx_rrc[,i]))]<-""
}
qx_rrc<-data.frame(qx_rrc)
qx_rrc$X<-as.integer(as.character(qx_rrc$X))
#########################################################################################################
##################################################第三部分：数据匹配#####################################
source(paste0(deep_local,"yck_wash_car_model\\config\\config_fun\\fun_match.R",sep=""),echo=TRUE,encoding="utf-8")
source(paste0(deep_local,"yck_wash_car_model\\config\\config_fun\\fun_iteration.R",sep=""),echo=TRUE,encoding="utf-8")
source(paste0(deep_local,"yck_wash_car_model\\config\\config_fun\\fun_match_result.R",sep=""),echo=TRUE,encoding="utf-8")
##调用函数计算结果列表  data_input<-qx_rrc
list_result<-fun_match_result(che300,qx_rrc)
confidence<-list_result$confidence
return_db<-list_result$return_db
match_right<-list_result$match_right
match_repeat<-list_result$match_repeat
match_not<-list_result$match_not
return_db<-data.frame(car_platform="rrc",return_db)
return_db$id_che300<-as.integer(as.character(return_db$id_che300))
###输出已经匹配的人人车配置表
out_rrc<-read.csv(paste0(deep_local,"yck_wash_car_model\\config\\config_file\\out_rrc.csv",sep=""),header = T,sep = ",")
out_rrc_append<-unique(inner_join(match_right,qx_rrc,c("id_data_input"="X"))%>%dplyr::select(id_che300,cname=car_model_name,car_year,car_name,car_series1,car_price))
out_rrc<-unique(rbind(out_rrc,out_rrc_append))
write.csv(out_rrc,paste0(deep_local,"yck_wash_car_model\\config\\config_file\\out_rrc.csv",sep=""),row.names = F)
##平台输出
write.csv(return_db,paste0(deep_local,"yck_wash_car_model\\file\\output\\rrc.csv",sep=""),row.names = F)
###日志文件
rizhi<-data.frame(platform=unique(return_db$car_platform),
                  accurate=round(accurate[2]/(accurate[1]+accurate[2]),3),
                  n_right=nrow(match_right),n_repeat=nrow(match_repeat),
                  n_not=nrow(match_not),add_date=Sys.Date())
loc_channel<-dbConnect(MySQL(),user = "root",host="192.168.0.111",password= "000000",dbname="yck-data-center")
dbSendQuery(loc_channel,'SET NAMES gbk')
dbWriteTable(loc_channel,"analysis_match_id_tab",rizhi,append=T,row.names=F)
dbDisconnect(loc_channel)