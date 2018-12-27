###############################################综合所有的平台数据清洗程序############################
#清除缓存
rm(list = ls(all=T))
gc()
##文件主路径（移动文件，变更下一行路径即可）
price_model_loc<-gsub("\\/main|\\/bat","",tryCatch(dirname(rstudioapi::getActiveDocumentContext()$path),error=function(e){getwd()}))
lf<-list.files(paste0(price_model_loc,"/file/output",sep=""), full.names = T,pattern = "csv")
file.remove(lf)
##删除##清除csv文件
lf<-list.files(paste0(price_model_loc,"/file/output_final",sep=""), full.names = T,pattern = "csv")
file.remove(lf)

###################第一部分（基本输出）：各个平台数据分析
car_platform<-c('rrc.R','che168.R','csp.R','che58.R','guazi.R','youxin.R','czb.R','yiche.R')
for (i in 1:length(car_platform)) {
  tryCatch({source(paste0(paste0(gsub("\\/main|\\/bat","",tryCatch(dirname(rstudioapi::getActiveDocumentContext()$path),error=function(e){getwd()})),"/main/main_detail/",sep=""),car_platform[i],sep=""),echo=TRUE,encoding="utf-8")},
           error=function(e){print(paste0("错误!"))},
           finally={print(paste0("进程完成!"))})
  car_platform<-c('rrc.R','che168.R','csp.R','che58.R','guazi.R','youxin.R','czb.R','yiche.R')
}
######暂停###
###source("E:/Work_table/Study/Rexam/YCK/wash_car_model/main/main_detail/souche.R",echo=TRUE,encoding="utf-8")


##################第二部分（最终输出等待入库）：所有平台数据拼接
rm(list = ls(all=T))
gc()
library(stringr)
library(RMySQL)
##拼接
files_full <- list.files(paste0(gsub("\\/main|\\/bat","",tryCatch(dirname(rstudioapi::getActiveDocumentContext()$path),error=function(e){getwd()})),"/file/output",sep=""), full.names = T,pattern = "csv")
output_final <- data.frame()
for (i in 1:length(files_full)) {
  linshi<-read.csv(files_full[i],header=T,sep = ",")
  output_final <- rbind(output_final, linshi)
}
output_final<-data.frame(output_final,date_add=format(Sys.time(),'%Y-%m-%d'))
write.csv(output_final,paste0(gsub("\\/main|\\/bat","",tryCatch(dirname(rstudioapi::getActiveDocumentContext()$path),error=function(e){getwd()})),"/file/output_final/output.csv",sep=""),
          row.names = F,fileEncoding = "UTF-8",quote = F)
##数据入库
if(length(files_full)>=3){
  loc_channel<-dbConnect(MySQL(),user = "root",host="192.168.0.111",password= "000000",dbname="yck-data-center")
  dbSendQuery(loc_channel,'SET NAMES gbk')
  dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE '",gsub("\\/main|\\/bat","",tryCatch(dirname(rstudioapi::getActiveDocumentContext()$path),error=function(e){getwd()})),"/file/output_final/output.csv'",
                                 " INTO TABLE analysis_match_id CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;",sep=""))
  dbDisconnect(loc_channel)
  
  loc_channel<-dbConnect(MySQL(),user = "yckdc",host="47.106.189.86",password= "YckDC888",dbname="yck-data-center")
  dbSendQuery(loc_channel,'SET NAMES gbk')
  dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE '",gsub("\\/main|\\/bat","",tryCatch(dirname(rstudioapi::getActiveDocumentContext()$path),error=function(e){getwd()})),"/file/output_final/output.csv'",
                                 " INTO TABLE analysis_match_id CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;",sep=""))
  dbDisconnect(loc_channel)
  ###记录日志
  write.table(paste0("match表 ",Sys.Date(),"   成功！"),
              paste0(gsub("\\/main|\\/bat","",tryCatch(dirname(rstudioapi::getActiveDocumentContext()$path),error=function(e){getwd()})),"/file/output/suc_or_los.txt",sep=""),row.names = F,col.names=F,append=T)
}else{
  write.table(paste0("match表 ",Sys.Date(),"   程序中断！"),
              paste0(gsub("\\/main|\\/bat","",tryCatch(dirname(rstudioapi::getActiveDocumentContext()$path),error=function(e){getwd()})),"/file/output/suc_or_los.txt",sep=""),row.names = F,col.names=F,append=T)
}


##################第二部分（最终输出等待入库）：将match_id中的right数据放入宽表
source(paste0(gsub("\\/main|\\/bat","",tryCatch(dirname(rstudioapi::getActiveDocumentContext()$path),error=function(e){getwd()})),"/main/analysis_wide_table/analysis_wide_table.R",sep=""),echo=TRUE,encoding="utf-8")
library(RMySQL)
if(length(unique(wutb$car_platform))>=1){
  loc_channel<-dbConnect(MySQL(),user = "root",host="192.168.0.111",password= "000000",dbname="yck-data-center")
  dbSendQuery(loc_channel,'SET NAMES gbk')
  dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE '",gsub("\\/main|\\/bat","",tryCatch(dirname(rstudioapi::getActiveDocumentContext()$path),error=function(e){getwd()})),"/file/output_final/analysis_wide_table.csv'",
                                 " INTO TABLE analysis_wide_table CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;",sep=""))
  dbDisconnect(loc_channel)
  
  
  loc_channel<-dbConnect(MySQL(),user = "yckdc",host="47.106.189.86",password= "YckDC888",dbname="yck-data-center")
  dbSendQuery(loc_channel,'SET NAMES gbk')
  dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE '",gsub("\\/main|\\/bat","",tryCatch(dirname(rstudioapi::getActiveDocumentContext()$path),error=function(e){getwd()})),"/file/output_final/analysis_wide_table.csv'",
                                 " INTO TABLE analysis_wide_table CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;",sep=""))
  dbDisconnect(loc_channel)
  ###记录日志
  write.table(paste0("wide表 ",Sys.Date(),"   成功！"),
              paste0(gsub("\\/main|\\/bat","",tryCatch(dirname(rstudioapi::getActiveDocumentContext()$path),error=function(e){getwd()})),"/file/output/suc_or_los.txt",sep=""),row.names = F,col.names=F,append=T)
}else{
  write.table(paste0("wide表 ",Sys.Date(),"   程序中断！"),
              paste0(gsub("\\/main|\\/bat","",tryCatch(dirname(rstudioapi::getActiveDocumentContext()$path),error=function(e){getwd()})),"/file/output/suc_or_los.txt",sep=""),row.names = F,col.names=F,append=T)
}