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
yck_yiche<-dbFetch(dbSendQuery(loc_channel,"SELECT id car_id,brand brand_name,series series_name,model model_name,msrp model_price FROM spider_www_yiche a 
                               INNER JOIN (SELECT a.id_data_input FROM analysis_match_id a where a.car_platform='yiche' AND a.match_des='not') m ON a.id=m.id_data_input;"),-1)
dbDisconnect(loc_channel)
rm_rule<- read.csv(paste0(deep_local,"yck_wash_car_model\\config\\config_file\\reg_rule.csv",sep=""),header = T,sep = ",")
rm_series_rule<- read.csv(paste0(deep_local,"yck_wash_car_model\\config\\config_file\\reg_series_rule.csv",sep=""),header = T,sep = ",")
out_rrc<- read.csv(paste0(deep_local,"yck_wash_car_model\\config\\config_file\\out_rrc.csv",sep=""),header = T,sep = ",")
loc_channel<-dbConnect(MySQL(),user = "root",host="192.168.0.111",password= "000000",dbname="yck-data-center")
dbSendQuery(loc_channel,'SET NAMES gbk')
che300<-dbFetch(dbSendQuery(loc_channel,"SELECT * FROM analysis_che300_cofig_info;"),-1)
dbDisconnect(loc_channel)
source(paste0(deep_local,"yck_wash_car_model\\config\\config_fun\\fun_stopWords.R",sep=""),echo=TRUE,encoding="utf-8")
source(paste0(deep_local,"yck_wash_car_model\\config\\config_fun\\fun_normalization.R",sep=""),echo=TRUE,encoding="utf-8")

######################------第一部分：得到车的品牌及车系-------#################
##临时车名
input_test1<-yck_yiche
input_test1$brand_name<-gsub(" ","",input_test1$brand_name)
input_test1$series_name<-gsub(" ","",input_test1$series_name)
input_test1$model_name<-gsub(" ","",input_test1$model_name)
#input_test1清洗
input_test1$brand_name<-fun_normalization(input_test1$brand_name)
input_test1$series_name<-fun_normalization(input_test1$series_name)
input_test1$model_name<-fun_normalization(input_test1$model_name)
input_test1$model_name<-gsub("马自达(3|)昂克赛拉|昂克赛拉","马自达3昂克赛拉",input_test1$model_name)
input_test1$model_name<-gsub("2012年|-2011 款","",input_test1$model_name)
input_test1$model_name<-gsub("神行者2|神行者","神行者2",input_test1$model_name)
input_test1$model_name<-gsub(c(str_c(1990:2030,"款",sep="",collapse='|')),"",input_test1$model_name)
input_test1$series_name<-gsub("三厢|两厢","",input_test1$series_name)
input_test1$car_id<-as.integer(input_test1$car_id)
rm_series_rule$qx_series_all<-as.character(rm_series_rule$qx_series_all)
rm_series_rule$series<-as.character(rm_series_rule$series)

############0920##############
input_test1$car_id<-as.integer(input_test1$car_id)
rm_series_rule$qx_series_all<-as.character(rm_series_rule$qx_series_all)
rm_series_rule$series<-as.character(rm_series_rule$series)
##补充新的name
brand_name<-str_extract(input_test1$brand_name,c(str_c(rm_series_rule$rule_name,sep="",collapse = "|")))
linshi_series<-c(str_c(rm_series_rule$rule_series,sep="",collapse = "|"))
linshi1<-data.frame(series=str_extract(paste(input_test1$brand_name,input_test1$series_name,input_test1$model_name,sep = "")[which(is.na(brand_name))],gsub(" ","",linshi_series)))
linshi1$series<-as.character(linshi1$series)
linshi1<-right_join(rm_series_rule,linshi1,by="series")%>%dplyr::select(name)
brand_name[which(is.na(brand_name))]<-as.character(linshi1$name)
brand_name[which(is.na(brand_name))]<-input_test1$brand_name[which(is.na(brand_name))]
##补充新的series
series_name<-str_extract(input_test1$model_name,gsub(" ","",linshi_series))
input_test1$model_name<-paste(input_test1$brand_name,input_test1$series_name,input_test1$model_name,sep = "")
series_name[which(is.na(series_name))]<-str_extract(input_test1$model_name[which(is.na(series_name))],gsub(" ","",linshi_series))
series_name[which(is.na(series_name))]<-input_test1$series_name[which(is.na(series_name))]
linshi<-str_extract(input_test1$model_name,"A佳|N3佳|N3")
linshi[which(is.na(linshi))]<-""
linshi[-grep("夏利",series_name)]<-""
series_name<-paste(series_name,linshi,sep = "")
series_name<-gsub("A佳A佳","A佳",series_name)
series_name<-gsub("N3佳N3佳","N3佳",series_name)
series_name<-gsub("N3N3","N3",series_name)
series_name[grep("赛欧3",input_test1$model_name)]<-str_extract(input_test1$model_name[grep("赛欧3",input_test1$model_name)],"赛欧3")
####------重新将新的brand及series替换------##
input_test1$brand_name<-brand_name
input_test1$series_name<-series_name
input_test1$brand_name[grep("奔奔MINI",input_test1$model_name)]<-"长安轿车"
input_test1$brand_name[grep("瑞风S2MINI",input_test1$model_name)]<-"江淮"
input_test1$brand_name[grep("新豹MINI",input_test1$model_name)]<-"长安商用"
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
data_input_0<-inner_join(data_input_0,yck_yiche,by="car_id")
data_input_0<-inner_join(data_input_0,rm_series_rule,by="id")%>%
  dplyr::select(car_id,brand_name=name,series_name=series,model_name,model_price)
data_input_0<-rbind(data_input_0,input_test1)
linshi<-str_extract(data_input_0$model_name,"进口")
linshi[which(is.na(linshi))]<-""
linshi<-gsub("进口","-进口",linshi)
data_input_0$series_name<-paste(data_input_0$series_name,linshi,sep = "")
data_input_0$model_name<-toupper(data_input_0$model_name)
data_input_0<-data.frame(data_input_0,discharge_standard="",liter="",auto="")
#---准确性accurate
accurate<-c(nrow(a3),nrow(yck_yiche))
rm(a1,a2,a3,input_test1,qx_series_all,qx_series_des,linshi,linshi_series,brand_name)
gc()


######################------第二部分：清洗model_name-------#################
#-----数据转换名称------
data_input<-data_input_0
data_input$model_price<-as.integer(data_input$model_price)
qx_name<-toupper(data_input$model_name)
qx_name<-trim(qx_name)
qx_name<-gsub(" +"," ",qx_name)
car_model_name<-data_input$model_name
##---------part1：得到年款及指导价--------------car_model_name[-grep("",car_year)]
qx_name<-gsub("2012年","2012款",qx_name)
qx_name<-gsub("-2011 款","2011款 ",qx_name)
car_year<-str_extract(qx_name,c(str_c(1990:2030,"款",sep="",collapse='|')))
car_year<-gsub("款","",car_year)
loc_year<-str_locate(qx_name,c(str_c(1990:2030,"款",sep="",collapse='|')))
car_price<-round(data_input$model_price/10000,2)
qx_name<-str_sub(qx_name,loc_year[,2]+1)
qx_name<-gsub("\\（","(",qx_name)
qx_name<-gsub("\\）",")",qx_name)
qx_name<-gsub("\\(进口\\)","",qx_name)
qx_name<-gsub("\\(海外\\)","",qx_name)
qx_name<-gsub("POWER DAILY","宝迪",qx_name)
qx_name<-gsub("III","Ⅲ",qx_name)
qx_name<-gsub("II","Ⅱ",qx_name)
qx_name<-gsub("—|－","-",qx_name)
qx_name<-gsub("\\·|\\?|(-|)JL465Q","",qx_name)
qx_name<-gsub("ONE\\+","ONE佳",qx_name)
qx_name<-gsub("劲能版\\+","劲能版佳",qx_name)
qx_name<-gsub("选装(包|)","佳",qx_name)
qx_name<-gsub("格锐","格越",qx_name)
qx_name<-gsub("北京现代","现代",qx_name)
qx_name<-gsub("2\\/3|2\\+2\\+3|2\\+3\\+2","",qx_name)
qx_name<-fun_normalization(qx_name)

###
car_name<-data_input$brand_name
car_series1<-data_input$series_name
car_series1<-gsub("\\（","(",car_series1)
car_series1<-gsub("\\）",")",car_series1)
car_series1<-gsub("\\(进口\\)|\\(海外\\)|-进口|-海外","",car_series1)
qx_name<-paste(car_name,car_series1,qx_name,sep = "")
seriesFun<-function(i){
  sub(car_name[i],"",car_series1[i])
}
series_des<-unlist(lapply(1:length(car_name),seriesFun))
series_des<-trim(series_des)
series_all<-paste(car_name,series_des,sep = "")
##1、清洗全称
seriesFun<-function(i){
  gsub(series_all[i],"",qx_name[i])
}
qx_name<-unlist(lapply(1:length(car_name),seriesFun))
##2、清洗系列
seriesFun<-function(i){
  gsub(car_series1[i],"",qx_name[i])
}
qx_name<-unlist(lapply(1:length(car_name),seriesFun))
##3、清洗品牌
seriesFun<-function(i){
  gsub(car_name[i],"",qx_name[i])
}
qx_name<-unlist(lapply(1:length(car_name),seriesFun))

car_name<-data_input$brand_name
car_series1<-data_input$series_name

##--------------------停用词清洗--------------------
####################################################CROSS
###-------词语描述归一-----
output_data<-fun_stopWords(data_input,qx_name)
#------------------数据保存----------------
qx_yiche<-data.frame(X= data_input_0$car_id,output_data)

#清洗多余空格
qx_yiche<-trim(qx_yiche)
qx_yiche$car_model_name<-gsub(" +"," ",qx_yiche$car_model_name)
qx_yiche<-sapply(qx_yiche,as.character)
for (i in 1:dim(qx_yiche)[2]) {
  qx_yiche[,i][which(is.na(qx_yiche[,i]))]<-""
}
qx_yiche<-data.frame(qx_yiche)
qx_yiche$X<-as.integer(as.character(qx_yiche$X))
data_input<-qx_yiche
#########################################################################################################
##################################################第二大章：数据匹配#####################################
source(paste0(deep_local,"yck_wash_car_model\\config\\config_fun\\fun_match.R",sep=""),echo=TRUE,encoding="utf-8")
source(paste0(deep_local,"yck_wash_car_model\\config\\config_fun\\fun_iteration.R",sep=""),echo=TRUE,encoding="utf-8")
source(paste0(deep_local,"yck_wash_car_model\\config\\config_fun\\fun_match_result_yiche.R",sep=""),echo=TRUE,encoding="utf-8")
##调用函数计算结果列表
list_result<-fun_match_result_yiche(che300,qx_yiche)
confidence<-list_result$confidence
return_db<-list_result$return_db
match_right<-list_result$match_right
match_repeat<-list_result$match_repeat
match_not<-list_result$match_not
return_db<-data.frame(car_platform="yiche",return_db)
return_db$id_che300<-as.integer(as.character(return_db$id_che300))
write.csv(return_db,paste0(deep_local,"yck_wash_car_model\\file\\output\\yiche_not.csv",sep=""),row.names = F)
###日志文件
rizhi<-data.frame(platform=unique(return_db$car_platform),
                  accurate=round(accurate[2]/(accurate[1]+accurate[2]),3),
                  n_right=nrow(match_right),n_repeat=nrow(match_repeat),
                  n_not=nrow(match_not),add_date=Sys.Date())
loc_channel<-dbConnect(MySQL(),user = "root",host="192.168.0.111",password= "000000",dbname="yck-data-center")
dbSendQuery(loc_channel,'SET NAMES gbk')
dbWriteTable(loc_channel,"analysis_match_id_tab",rizhi,append=T,row.names=F)
dbDisconnect(loc_channel)