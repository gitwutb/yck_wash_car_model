###############################################综合所有的平台数据清洗程序############################
#清除缓存
rm(list = ls(all=T))
gc()
##清除csv文件
lf<-list.files("E:/Work_table/gitwutb/git_project/yck_wash_car_model/file/output", full.names = T,pattern = "csv")
file.remove(lf)
##删除
lf<-list.files("E:/Work_table/gitwutb/git_project/yck_wash_car_model/file/output_final", full.names = T,pattern = "csv")
file.remove(lf)


###################第一部分（基本输出）：各个平台数据分析
car_platform<-c('rrc_not.R','che168_not.R','csp_not.R','che58_not.R','guazi_not.R','youxin_not.R','czb_not.R','yiche_not.R')
for (i in 1:length(car_platform)) {
  tryCatch({source(paste0("E:/Work_table/gitwutb/git_project/yck_wash_car_model/main/main_detail_not/",car_platform[i],sep=""),echo=TRUE,encoding="utf-8")},
           error=function(e){cat(write.table(data.frame(platform=paste0('iserror'),data=Sys.Date()),
                                             paste0(deep_local,"wash_car_model/file/output/rizhi_error.txt",sep=""),col.names = F,row.names = F,append=T),conditionMessage(e),"\n\n")},
           finally={print(paste0("进程完成!"))})
  car_platform<-c('rrc_not.R','che168_not.R','csp_not.R','che58_not.R','guazi_not.R','youxin_not.R','czb_not.R','yiche_not.R')
}

# source("E:/Work_table/gitwutb/git_project/yck_wash_car_model/main/main_detail/rrc.R",echo=TRUE,encoding="utf-8")
# source("E:/Work_table/gitwutb/git_project/yck_wash_car_model/main/main_detail/che168.R",echo=TRUE,encoding="utf-8")
# source("E:/Work_table/gitwutb/git_project/yck_wash_car_model/main/main_detail/csp.R",echo=TRUE,encoding="utf-8")
# source("E:/Work_table/gitwutb/git_project/yck_wash_car_model/main/main_detail/che58.R",echo=TRUE,encoding="utf-8")
# source("E:/Work_table/gitwutb/git_project/yck_wash_car_model/main/main_detail/guazi.R",echo=TRUE,encoding="utf-8")
# source("E:/Work_table/gitwutb/git_project/yck_wash_car_model/main/main_detail/youxin.R",echo=TRUE,encoding="utf-8")
# source("E:/Work_table/gitwutb/git_project/yck_wash_car_model/main/main_detail/czb.R",echo=TRUE,encoding="utf-8")
# source("E:/Work_table/gitwutb/git_project/yck_wash_car_model/main/main_detail/yiche.R",echo=TRUE,encoding="utf-8")
######暂停###
###source("E:/Work_table/gitwutb/git_project/yck_wash_car_model/main/main_detail/souche.R",echo=TRUE,encoding="utf-8")


##################第二部分（最终输出等待入库）：所有平台数据拼接
rm(list = ls(all=T))
gc()
library(stringr)
library(RMySQL)
##拼接
files_full <- list.files("E:/Work_table/gitwutb/git_project/yck_wash_car_model/file/output", full.names = T,pattern = "csv")
output_final <- data.frame()
for (i in 1:length(files_full)) {
  linshi<-read.csv(files_full[i],header=T,sep = ",")
  output_final <- rbind(output_final, linshi)
}
output_final<-data.frame(output_final,date_add=format(Sys.time(),'%Y-%m-%d'))
write.csv(output_final,"E:/Work_table/gitwutb/git_project/yck_wash_car_model/file/output_final/output.csv",
          row.names = F,fileEncoding = "UTF-8",quote = F)
##数据入库
if(length(files_full)>=4){
  loc_channel<-dbConnect(MySQL(),user = "root",host="192.168.0.111",password= "000000",dbname="yck-data-center")
  dbSendQuery(loc_channel,'SET NAMES gbk')
  dbSendQuery(loc_channel,"LOAD DATA LOCAL INFILE 'E:/Work_table/Study/Rexam/YCK/wash_car_model/file/output_final/output.csv'
              INTO TABLE analysis_match_id CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;")
  dbDisconnect(loc_channel)
  ###记录日志
  write.table(paste0("match表 ",Sys.Date(),"   成功！"),
              "E:/Work_table/gitwutb/git_project/yck_wash_car_model/file/output/suc_or_los.txt",row.names = F,col.names=F,append=T)
}else{
  write.table(paste0("match表 ",Sys.Date(),"   程序中断！"),
              "E:/Work_table/gitwutb/git_project/yck_wash_car_model/file/output/suc_or_los.txt",row.names = F,col.names=F,append=T)
}


##################第二部分（最终输出等待入库）：将match_id中的right数据放入宽表
source("E:/Work_table/gitwutb/git_project/yck_wash_car_model/main/analysis_wide_table/analysis_wide_table.R",echo=TRUE,encoding="utf-8")
library(RMySQL)
if(length(unique(wutb$car_platform))>=4){
  loc_channel<-dbConnect(MySQL(),user = "root",host="192.168.0.111",password= "000000",dbname="yck-data-center")
  dbSendQuery(loc_channel,'SET NAMES gbk')
  dbSendQuery(loc_channel,"LOAD DATA LOCAL INFILE 'E:/Work_table/Study/Rexam/YCK/wash_car_model/file/output_final/analysis_wide_table.csv'
              INTO TABLE analysis_wide_table CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;")
  dbDisconnect(loc_channel)
  ###记录日志
  write.table(paste0("wide表 ",Sys.Date(),"   成功！"),
              "E:/Work_table/gitwutb/git_project/yck_wash_car_model/file/output/suc_or_los.txt",row.names = F,col.names=F,append=T)
}else{
  write.table(paste0("wide表 ",Sys.Date(),"   程序中断！"),
              "E:/Work_table/gitwutb/git_project/yck_wash_car_model/file/output/suc_or_los.txt",row.names = F,col.names=F,append=T)
}