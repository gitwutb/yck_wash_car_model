#清除缓存
rm(list = ls(all=T))
gc()
library(dplyr)
library(stringr)
library(raster)
#help(package="dplyr")
#读取数据
library(RMySQL)
deep_local<-gsub("\\/bat|\\/main.*","",tryCatch(dirname(rstudioapi::getActiveDocumentContext()$path),error=function(e){getwd()}))
loc_channel<-dbConnect(MySQL(),user = "root",host="192.168.0.111",password= "000000",dbname="yck-data-center")
dbSendQuery(loc_channel,'SET NAMES gbk')
table.name<-dbListTables(loc_channel)
# field.name<-dbListFields(loc_channel,"")
yck_czp<-dbFetch(dbSendQuery(loc_channel,"SELECT id car_id,brand brand_name,series series_name,model model_name,emission discharge_standard,displacement liter FROM spider_www_souche a
                             WHERE a.id>(SELECT max_id FROM analysis_match_idmax_temp WHERE car_platform in ('souche'));"),-1)
rm_series_rule<-dbFetch(dbSendQuery(loc_channel,"SELECT * FROM config_reg_series_rule;"),-1)
che300<-dbFetch(dbSendQuery(loc_channel,"SELECT * FROM analysis_che300_cofig_info;"),-1)
dbDisconnect(loc_channel)
rm_rule<- read.csv(paste0(deep_local,"\\config\\config_file\\reg_rule.csv",sep=""),header = T,sep = ",")
out_rrc<- read.csv(paste0(deep_local,"\\config\\config_file\\out_rrc.csv",sep=""),header = T,sep = ",")
source(paste0(deep_local,"\\config\\config_fun\\fun_stopWords.R",sep=""),echo=TRUE,encoding="utf-8")
source(paste0(deep_local,"\\config\\config_fun\\fun_normalization.R",sep=""),echo=TRUE,encoding="utf-8")
yck_czp<-data.frame(yck_czp,model_price="",car_auto="")


######################------第一部分：得到车的品牌及车系-------#################
##临时车名
input_test1<-yck_czp
input_test1$model_name<-gsub(" ","",input_test1$model_name)
#input_test1清洗
input_test1$model_name<-gsub("2012年|-2011 款","",input_test1$model_name)
input_test1$model_name<-gsub(c(str_c(1990:2030,"款",sep="",collapse='|')),"",input_test1$model_name)
input_test1$model_name<-fun_normalization(input_test1$model_name)
input_test1$model_name<-gsub("昂科赛拉","昂克赛拉",input_test1$model_name)
input_test1$model_name<-gsub("马自达(3|)昂克赛拉|昂克赛拉","马自达3昂克赛拉",input_test1$model_name)
input_test1$model_name<-gsub("神行者2|神行者","神行者2",input_test1$model_name)
input_test1$model_name[grep("赛欧.*赛欧3",input_test1$model_name)]<-sub("赛欧","",input_test1$model_name[grep("赛欧.*赛欧3",input_test1$model_name)])
input_test1$model_name[grep("赛欧200(1|2|3|4)|200(1|2|3|4).*赛欧",input_test1$model_name)]<-"别克赛欧"
input_test1$car_id<-as.integer(input_test1$car_id)
rm_series_rule$qx_series_all<-as.character(rm_series_rule$qx_series_all)
rm_series_rule$series<-as.character(rm_series_rule$series)

###input_test2<-input_test1    input_test1<-input_test2
###----------------前期准备：提取准确的brand和series-----------
brand_name<-str_extract(input_test1$model_name,c(str_c(unique(rm_series_rule$rule_name),sep="",collapse = "|")))
brand_name[which(is.na(brand_name))]<-""
linshi_series<-c(str_c(rm_series_rule$rule_series,sep="",collapse = "|"))
series_name<-str_extract(input_test1$model_name,gsub(" ","",linshi_series))
series_name[which(is.na(series_name))]<-""
linshi<-str_extract(input_test1$model_name,"A佳|N3佳|-进口")
linshi[which(is.na(linshi))]<-""
series_name<-paste(series_name,linshi,sep = "")
####------重新将新的brand及series替换------##
input_test1$brand_name<-brand_name
input_test1$series_name<-series_name
input_test1$brand_name[grep("奔奔MINI",input_test1$model_name)]<-"长安轿车"
input_test1$brand_name[grep("瑞风S2MINI",input_test1$model_name)]<-"江淮"
input_test1$brand_name[grep("新豹MINI",input_test1$model_name)]<-"长安商用"


###----------------第一步：分离-----------#######
car_name_info<-str_extract(paste(input_test1$model_name,sep = ""),c(str_c(rm_series_rule$qx_series_all,sep="",collapse = "|")))
a1<-data.frame(input_test1[grep("",car_name_info),],qx_series_all=car_name_info[grep("",car_name_info)])
a2<-data.frame(car_id=as.integer(setdiff(input_test1$car_id,a1$car_id)))
input_test1<-inner_join(input_test1,a2,by="car_id")
#全称匹配到车300
a1$qx_series_all<-as.character(a1$qx_series_all)
a1<-inner_join(a1,rm_series_rule,by="qx_series_all")%>%
  dplyr::select(car_id,brand_name,series_name,model_name,model_price,id,name,series,qx_series_all,discharge_standard,car_auto,liter)
###----------------第二步：对剩余部分进行全称及系列名匹配-----------
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
a2<-inner_join(input_test1,rm_series_rule,by="qx_series_all")%>%
  dplyr::select(car_id,brand_name,series_name,model_name,model_price,id,name,series,qx_series_all,discharge_standard,car_auto,liter)
###----------------第三步：model_name匹配系列（存在别克赛欧-雪佛兰赛欧等）-----------
a3<-data.frame(car_id=as.integer(setdiff(input_test1$car_id,a2$car_id)))
input_test1<-inner_join(input_test1,a3,by="car_id")%>%dplyr::select(-qx_series_all)
car_name_info<-str_extract(paste(input_test1$series_name,input_test1$model_name,sep = ""),gsub(" ","",c(str_c(rm_series_rule$series,sep="",collapse = "|"))))
a3<-data.frame(input_test1[grep("",car_name_info),],series_t=car_name_info[grep("",car_name_info)])
#全称匹配xilie到车300gsub(" ","",c(str_c(rm_series_rule$series,sep="",collapse = "|")))
a3$series_t<-as.character(a3$series_t)
rm_series_rule$series_t<-as.character(rm_series_rule$series_t)
a3<-inner_join(a3,rm_series_rule,c("series_t"="series_t"))%>%
  dplyr::select(car_id,brand_name,series_name,model_name,model_price,id,name,series,qx_series_all,discharge_standard,car_auto,liter)
##-----------------第四步：未匹配上a4-----------########
a4<-data.frame(car_id=setdiff(input_test1$car_id,a3$car_id))
if(nrow(a4)==0){
  data_input_0<-rbind(a1,a2,a3)
}else{
  a4<-data.frame(inner_join(a4,input_test1,c("car_id"="car_id")),id="",name="",series="",qx_series_all="")
  a4$name<-a4$brand_name
  a4$series<-a4$series_name
  data_input_0<-rbind(a1,a2,a3,a4)
}

########----组合所有car_id---###########
data_input_0<-inner_join(data_input_0,yck_czp[,c("car_id","brand_name","series_name","model_name")],by="car_id")%>%
  dplyr::select(car_id,brand_name=name,series_name=series,model_name=model_name.y,model_price,qx_series_all,discharge_standard,auto=car_auto)
data_input_0$model_name<-toupper(data_input_0$model_name)
data_input_0$auto<-gsub("-／","",data_input_0$auto)
linshi<-str_extract(data_input_0$model_name,"进口")
linshi[which(is.na(linshi))]<-""
linshi<-gsub("进口","-进口",linshi)
data_input_0$series_name<-paste(data_input_0$series_name,linshi,sep = "")
#---准确性accurate
accurate<-c(nrow(a4),nrow(yck_czp))
rm(a1,a2,a3,a4,car_name_info,input_test1,qx_series_all,qx_series_des)
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
car_year<-str_extract(qx_name,"[1-2][0-9][0-9][0-9]款|[0-9][0-9]款|2000")
car_year<-gsub("款","",car_year)
car_year[which(nchar(car_year)==2)]<-paste("20",car_year[which(nchar(car_year)==2)],sep = "")
loc_year<-str_locate(qx_name,"[1-2][0-9][0-9][0-9]款|[0-9][0-9]款|2000")
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
qx_name<-gsub("2\\/3|2\\+2\\+3|2\\+3\\+2","",qx_name)

car_name<-data_input$brand_name
car_series1<-gsub("-进口","",data_input$series_name)
forFun<-function(i){
  sub(paste0(".*",car_series1[i],sep=""),"",qx_name[i])
}
qx_name<-unlist(lapply(1:length(car_series1),forFun))
qx_name<-gsub("奔驰.*级 |奔驰.*级AMG|奔驰","",qx_name)
car_series1<-data_input$series_name

##--------------------停用词清洗--------------------
####################################################CROSS
###-------词语描述归一-----
output_data<-fun_stopWords(data_input,qx_name)
#------------------数据保存----------------
qx_souche<-data.frame(X=data_input_0$car_id,output_data)

#清洗多余空格
qx_souche<-trim(qx_souche)
qx_souche$car_model_name<-gsub(" +"," ",qx_souche$car_model_name)
qx_souche<-sapply(qx_souche,as.character)
for (i in 1:dim(qx_souche)[2]) {
  qx_souche[,i][which(is.na(qx_souche[,i]))]<-""
}
qx_souche<-data.frame(qx_souche)
qx_souche$X<-as.integer(as.character(qx_souche$X))
#剔除不包含的车系
df_filter<- gsub('-进口','',unique(qx_souche$car_series1)) %>% as.character()
che300<-che300 %>% filter(gsub('-进口','',car_series1)%in%df_filter)
#########################################################################################################
##################################################第二大章：数据匹配#####################################
source(paste0(deep_local,"\\config\\config_fun\\fun_match.R",sep=""),echo=TRUE,encoding="utf-8")
source(paste0(deep_local,"\\config\\config_fun\\fun_iteration.R",sep=""),echo=TRUE,encoding="utf-8")
source(paste0(deep_local,"\\config\\config_fun\\fun_match_result_czb.R",sep=""),echo=TRUE,encoding="utf-8")
data_input<-qx_souche
##调用函数计算结果列表
list_result<-fun_match_result_czb(che300,qx_souche)
confidence<-list_result$confidence
return_db<-list_result$return_db
match_right<-list_result$match_right
match_repeat<-list_result$match_repeat
match_not<-list_result$match_not
return_db<-data.frame(car_platform="souche",return_db)
return_db$id_che300<-as.integer(as.character(return_db$id_che300))
write.csv(return_db,paste0(deep_local,"\\file\\output\\souche.csv",sep=""),row.names = F)
###日志文件
rizhi<-data.frame(platform=unique(return_db$car_platform),
                  accurate=round(accurate[2]/(accurate[1]+accurate[2]),3),
                  n_right=nrow(match_right),n_repeat=nrow(match_repeat),
                  n_not=nrow(match_not),add_date=Sys.Date())
loc_channel<-dbConnect(MySQL(),user = "root",host="192.168.0.111",password= "000000",dbname="yck-data-center")
dbSendQuery(loc_channel,'SET NAMES gbk')
dbWriteTable(loc_channel,"log_analysis_match_id",rizhi,append=T,row.names=F)
dbDisconnect(loc_channel)