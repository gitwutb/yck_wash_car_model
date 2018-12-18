#清除缓存
rm(list = ls(all=T))
gc()
library(dplyr)
library(stringr)
library(raster)
#help(package="dplyr")
#读取数据
library(RMySQL)
#deep_local<-gsub("\\/config.*","",dirname(rstudioapi::getActiveDocumentContext()$path))
deep_local<-c("E:/Work_table/gitwutb/git_project/yck_wash_car_model")

car_platform<-c('plat_id_yiche.R','plat_id_czb.R','plat_id_autohome.R','plat_id_souhu.R')
for (i in 1:length(car_platform)) {
  tryCatch({source(paste0(paste0("E:/Work_table/gitwutb/git_project/yck_wash_car_model","\\config\\config_plat_id_match\\code\\",sep=""),car_platform[i],sep=""),echo=TRUE,encoding="utf-8")},
           error=function(e){cat(write.table(data.frame(platform=paste0('iserror'),data=Sys.Date()),
                                             paste0("E:/Work_table/gitwutb/git_project/yck_wash_car_model","\\config\\config_plat_id_match\\code\\p_error.txt",sep=""),col.names = F,row.names = F,append=T),conditionMessage(e),"\n\n")},
           finally={print(paste0("进程完成!"))})
  car_platform<-c('plat_id_yiche.R','plat_id_czb.R','plat_id_autohome.R','plat_id_souhu.R')
}


loc_channel<-dbConnect(MySQL(),user = "root",host="192.168.0.111",password= "000000",dbname="yck-data-center")
dbSendQuery(loc_channel,'SET NAMES gbk')
id_che300<-dbFetch(dbSendQuery(loc_channel,"SELECT * FROM analysis_che300_cofig_info;"),-1)%>%dplyr::select(id_che300=car_id)
dbDisconnect(loc_channel)

id_autohome<-read.csv(paste0(deep_local,"\\config\\config_plat_id_match\\out_autohome.csv"))
id_souhu<-read.csv(paste0(deep_local,"\\config\\config_plat_id_match\\out_souhu.csv"))
id_yiche<-read.csv(paste0(deep_local,"\\config\\config_plat_id_match\\out_yiche.csv"))
id_czb<-read.csv(paste0(deep_local,"\\config\\config_plat_id_match\\out_czb.csv"))
id_result<-left_join(left_join(left_join(left_join(id_che300,id_autohome,by="id_che300"),id_souhu,by="id_che300"),id_yiche,by="id_che300"),id_czb,by="id_che300")%>%unique()
for (i in 1:dim(id_result)[2]) {
  id_result[,i][which(is.na(id_result[,i]))]<-0
}

##入库
write.csv(id_result,paste0(deep_local,"\\config\\config_plat_id_match\\id_result.csv"),
          row.names = F,fileEncoding = "UTF-8",quote = F)
loc_channel<-dbConnect(MySQL(),user = "root",host="192.168.0.111",password= "000000",dbname="yck-data-center")
dbSendQuery(loc_channel,'SET NAMES gbk')
dbSendQuery(loc_channel,"TRUNCATE config_plat_id_match")
dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE '","E:/Work_table/gitwutb/git_project/yck_wash_car_model","/config/config_plat_id_match/id_result.csv'",
                               " INTO TABLE config_plat_id_match CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;",sep=""))
dbDisconnect(loc_channel)