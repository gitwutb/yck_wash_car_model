#清除缓存
#[\u4e00-\u9fa5]汉字
rm(list = ls(all=T))
gc()
library(dplyr)
library(stringr)
library(raster)
library(RMySQL)
deep_local<-gsub("\\/main|\\/bat","",tryCatch(dirname(rstudioapi::getActiveDocumentContext()$path),error=function(e){getwd()}))
rm_rule<- read.csv(paste0(deep_local,"\\config\\config_file\\reg_rule.csv",sep=""),header = T,sep = ",")
loc_channel<-dbConnect(MySQL(),user = "root",host="192.168.0.111",password= "000000",dbname="yck-data-center")
dbSendQuery(loc_channel,'SET NAMES gbk')
table.name<-dbListTables(loc_channel)
# field.name<-dbListFields(loc_channel,"")
data_che300<-dbFetch(dbSendQuery(loc_channel,"SELECT * FROM config_che300_major_info;"),-1)
data_che300<-data_che300%>%dplyr::select(-seat_number,-short_name)
dbDisconnect(loc_channel)
source(paste0(deep_local,"\\config\\config_fun\\fun_stopWords.R",sep=""),echo=TRUE,encoding="utf-8")

#-----数据转换名称------
data_input<-data_che300
qx_name<-toupper(data_input$model_name)
car_model_name<-data_input$model_name
car_id<-data_input$model_id

##---------第一部分：得到年款--------------
car_year<-data_input$model_year
car_price<-data_input$model_price
#清除年年款
qx_name<-gsub(c(str_c(1990:2030,"款 ",sep="",collapse='|')),"",qx_name)
qx_name<-gsub("\\（","(",qx_name)
qx_name<-gsub("\\）",")",qx_name)
qx_name<-gsub("\\(进口\\)","",qx_name)
qx_name<-gsub("\\(海外\\)","",qx_name)
qx_name<-gsub("POWER DAILY","宝迪",qx_name)
qx_name<-gsub("北京汽车","北汽",qx_name)
qx_name<-gsub("III","Ⅲ",qx_name)
qx_name<-gsub("II","Ⅱ",qx_name)
qx_name<-gsub("—|－","-",qx_name)
qx_name<-gsub("\\·|\\?","",qx_name)
qx_name<-gsub("ONE\\+","ONE佳",qx_name)
qx_name<-gsub("劲能版\\+","劲能版佳",qx_name)
qx_name<-gsub("选装(包|)","佳",qx_name)
qx_name<-gsub("格锐","格越",qx_name)
qx_name<-gsub("2\\/3|2\\+2\\+3|2\\+3\\+2","",qx_name)

##car_name---------得到品牌名-系列名-------
car_name<-toupper(data_input$brand_name)
car_series1<-toupper(data_input$series_name)
car_series1<-gsub("格锐","格越",car_series1)
car_series1<-gsub("\\（","(",car_series1)
car_series1<-gsub("\\）",")",car_series1)
linshi<-str_extract(car_series1,"进口|海外")
car_series1<-gsub("\\(进口\\)|\\(海外\\)","",car_series1)
forFun<-function(i){
  sub(car_series1[i],"",qx_name[i])
}
qx_name<-unlist(lapply(1:length(car_series1),forFun))
qx_name[grep("\\+",car_series1)]<-sub("\\+","",qx_name[grep("\\+",car_series1)])
linshi[-grep("",linshi)]<-""
car_series1<-paste(car_series1,"-",linshi,sep = "")
car_series1<-gsub("-$","",car_series1)
##--series停用词----
car_series1<-gsub("全新奔腾","奔腾",car_series1)
car_series1<-gsub("北京汽车|北京","",car_series1)
car_series1<-gsub("^JEEP$","北京JEEP",car_series1)
car_series1<-gsub("全新奔腾","奔腾",car_series1)
car_series1<-gsub("锋范经典","锋范",car_series1)
car_series1<-gsub("名爵ZS","MGZS",car_series1)
car_series1<-gsub("MINI |SMART ","",car_series1)
car_series1<-gsub("PASSAT","帕萨特",car_series1)
car_series1<-gsub("\\+","佳",car_series1)


##--------------------停用词清洗--------------------
####################################################CROSS
###-------词语描述归一-----
output_data<-fun_stopWords(data_input,qx_name)

#------------------数据保存----------------
qx_che300<-data.frame(car_id,output_data)
#清洗多余空格
qx_che300<-trim(qx_che300)
qx_che300$car_model_name<-gsub(" +"," ",qx_che300$car_model_name)
qx_che300<-sapply(qx_che300,as.character)
for (i in 1:dim(qx_che300)[2]) {
  qx_che300[,i][-grep("",qx_che300[,i])]<-""
}
write.csv(qx_che300,paste0(deep_local,"\\qx_che300.csv",sep=""),row.names = F,fileEncoding = "UTF-8",quote = F)
#入库
loc_channel<-dbConnect(MySQL(),user = "root",host="192.168.0.111",password= "000000",dbname="yck-data-center")
dbSendQuery(loc_channel,'SET NAMES gbk')
dbSendQuery(loc_channel,"TRUNCATE TABLE analysis_che300_cofig_info")
dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE ","'",paste0(deep_local,"/qx_che300.csv",sep=""),"'",
                               " INTO TABLE analysis_che300_cofig_info CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;"))
dbDisconnect(loc_channel)


##################----------------第二大部分：提取品牌系列等reg_series_rule.csv----------------------#########################
#清除缓存
#[\u4e00-\u9fa5]汉字
rm(list = ls(all=T))
gc()
library(dplyr)
library(stringr)
library(raster)
#读取数据
deep_local<-gsub("\\/main|\\/bat","",tryCatch(dirname(rstudioapi::getActiveDocumentContext()$path),error=function(e){getwd()}))
loc_channel<-dbConnect(MySQL(),user = "root",host="192.168.0.111",password= "000000",dbname="yck-data-center")
dbSendQuery(loc_channel,'SET NAMES gbk')
data_che300<-dbFetch(dbSendQuery(loc_channel,"SELECT * FROM analysis_che300_cofig_info;"),-1)
dbDisconnect(loc_channel)

#-----数据转换名称------
data_input<-data_che300

##car_name---------得到品牌名-系列名-------
car_name<-toupper(data_input$car_name)
car_series1<-toupper(data_input$car_series1)
car_series1<-gsub("-进口","",car_series1)

###########---------构建中间系列qx_series3----############
car_name<-gsub("\\·|\\?","",car_name)
seriesFun<-function(i){
  sub(car_name[i],"",car_series1[i])
}
qx_series_des<-unlist(lapply(1:length(car_series1),seriesFun))
qx_series_des<-trim(qx_series_des)
qx_series_all<-paste(car_name,qx_series_des,sep = "")
a<-which(nchar(qx_series_des)==0)
qx_series_des[a]<-car_name[a]

qx_series_des<-gsub(" ","",qx_series_des)
qx_series_all<-gsub(" ","",qx_series_all)

ce_data<-unique(data.frame(name=car_name,series=car_series1,series_t=car_series1,qx_series_all,qx_series_des))
ce_data<-ce_data[order(ce_data[,1],ce_data[,2],decreasing=T),]
ce_data$series_t<-gsub(" ","",ce_data$series_t)
##补齐
n<-nrow(ce_data)
rule_name<-as.character(unique(ce_data$name))
rule_name<-c(rule_name,rep(rule_name[length(rule_name)],n-length(rule_name)))
#series
rule_series<-as.character(unique(ce_data$series))
rule_series<-c("福田风景","别克赛欧",rule_series[order(rule_series,decreasing=T)])%>%unique()
rule_series<-c(rule_series,"MINI","中华")
rule_series<-c(rule_series,rep(rule_series[length(rule_series)],n-length(rule_series)))
ce_data<-data.frame(id=c(1:n),ce_data,rule_name,rule_series)

write.csv(ce_data,paste0(deep_local,"\\reg_series_rule.csv",sep=""),row.names = F,fileEncoding = "UTF-8",quote = F)
loc_channel<-dbConnect(MySQL(),user = "root",host="192.168.0.111",password= "000000",dbname="yck-data-center")
dbSendQuery(loc_channel,'SET NAMES gbk')
dbSendQuery(loc_channel,"TRUNCATE TABLE config_reg_series_rule")
dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE ","'",paste0(deep_local,"/reg_series_rule.csv",sep=""),"'",
                               " INTO TABLE config_reg_series_rule CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;"))
dbDisconnect(loc_channel)


#################**********云服务器**********#################
loc_channel<-dbConnect(MySQL(),user = "yckdc",host="47.106.189.86",password= "YckDC888",dbname="yck-data-center")
dbSendQuery(loc_channel,'SET NAMES gbk')
dbSendQuery(loc_channel,"TRUNCATE TABLE analysis_che300_cofig_info")
dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE ","'",paste0(deep_local,"/qx_che300.csv",sep=""),"'",
                               " INTO TABLE analysis_che300_cofig_info CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;"))
dbDisconnect(loc_channel)

loc_channel<-dbConnect(MySQL(),user = "yckdc",host="47.106.189.86",password= "YckDC888",dbname="yck-data-center")
dbSendQuery(loc_channel,'SET NAMES gbk')
dbSendQuery(loc_channel,"TRUNCATE TABLE config_reg_series_rule")
dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE ","'",paste0(deep_local,"/reg_series_rule.csv",sep=""),"'",
                               " INTO TABLE config_reg_series_rule CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;"))
dbDisconnect(loc_channel)
file.remove(c(paste0(deep_local,"\\reg_series_rule.csv",sep=""),paste0(deep_local,"\\qx_che300.csv",sep="")))