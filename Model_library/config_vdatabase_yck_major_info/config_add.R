##-------------每周一次增量更新-----------##
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
local_file<-gsub("(\\/main|\\/bat|\\/Model_library).*","",tryCatch(dirname(rstudioapi::getActiveDocumentContext()$path),error=function(e){getwd()}))
source(paste0(local_file,"/config/config_fun/fun_mysql_config_up.R"),echo=FALSE,encoding="utf-8")
local_defin<-fun_mysql_config_up()$local_defin
local_defin_yun<-fun_mysql_config_up()$local_defin_yun

####--车型库构建：品牌ID增加--####
loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
dbSendQuery(loc_channel,'SET NAMES gbk')
brand_add<-dbFetch(dbSendQuery(loc_channel,"SELECT a.Initial,a.brandid,a.brand_name FROM
        (SELECT DISTINCT Initial,brandid,brand_name FROM config_che300_major_info) a 
        LEFT JOIN config_vdatabase_yck_brand b ON a.brandid=b.brandid
        WHERE yck_brandid IS NULL;"),-1)
brand_all<-dbFetch(dbSendQuery(loc_channel,"SELECT a.Initial,a.brandid,a.brand_name,yck_brandid FROM
        (SELECT DISTINCT Initial,brandid,brand_name FROM config_che300_major_info) a 
        INNER JOIN config_vdatabase_yck_brand b ON a.brandid=b.brandid;"),-1)
dbDisconnect(loc_channel)
if(nrow(brand_add)>0){
  brand_all$yck_brandid<-gsub("[a-zA-Z]","",brand_all$yck_brandid)
  brand_Initial_max<-brand_all %>% group_by(Initial) %>% summarise(maxb=max(as.integer(yck_brandid))) %>%
    ungroup() %>% as.data.frame()
  brand_add<-left_join(brand_add,brand_Initial_max,by=c('Initial'))
  brand_add$maxb[is.na(brand_add$maxb)]<-0
  brand_add<-brand_add %>% group_by(Initial) %>% mutate(yck_brandid=paste0(Initial,maxb+as.integer(factor(brand_name)))) %>%
    as.data.frame() %>% dplyr::select(brandid,brand_name,yck_brandid) %>% mutate(car_country='无')
  write.csv(brand_add,paste0(local_file,"\\brand_add.csv",sep=""),row.names = F,fileEncoding = "UTF-8",quote = F)
  loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE ","'",paste0(local_file,"/brand_add.csv",sep=""),"'",
                                 " INTO TABLE config_vdatabase_yck_brand CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;"))
  dbDisconnect(loc_channel)
  loc_channel<-dbConnect(MySQL(),user = local_defin_yun$user,host=local_defin_yun$host,password= local_defin_yun$password,dbname=local_defin_yun$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE ","'",paste0(local_file,"/brand_add.csv",sep=""),"'",
                                 " INTO TABLE config_vdatabase_yck_brand CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;"))
  dbDisconnect(loc_channel)
}

####--车型库构建：车系ID增加--####
loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
dbSendQuery(loc_channel,'SET NAMES gbk')
series_add<-dbFetch(dbSendQuery(loc_channel,"SELECT a.series_id,a.series_group_name,a.series_name,yck_brandid FROM
        (SELECT DISTINCT brandid,series_id,series_group_name,series_name FROM config_che300_major_info) a 
        LEFT JOIN config_vdatabase_yck_series b ON a.series_id=b.series_id
        INNER JOIN config_vdatabase_yck_brand c ON a.brandid=c.brandid
        WHERE yck_seriesid IS NULL;"),-1)
series_all<-dbFetch(dbSendQuery(loc_channel,"SELECT yck_brandid,yck_seriesid FROM
        (SELECT DISTINCT brandid,series_id,series_name FROM config_che300_major_info) a 
        INNER JOIN config_vdatabase_yck_series b ON a.series_id=b.series_id
        INNER JOIN config_vdatabase_yck_brand c ON a.brandid=c.brandid;"),-1)
dbDisconnect(loc_channel)
if(nrow(series_add)>0){
  series_add$series_group_name[grep('进口',series_add$series_name)]<-
    paste0('进口',series_add$series_group_name[grep('进口',series_add$series_name)])
  series_add$series_group_name<-gsub('进口进口进口|进口进口','进口',series_add$series_group_name)
  series_add$series_name<-gsub('\\(进口)','',series_add$series_name)
  series_add$series_name<-gsub('欧尚COSMOS\\(科尚\\)','欧尚COSMOS-科尚',series_add$series_name)
  series_add$series_name<-toupper(series_add$series_name)
  is_import<-str_extract(series_add$series_group_name,'进口')
  is_import[is_import=='进口']<-1
  is_import[which(is.na(is_import))]<-0
 
  series_all$yck_seriesid<-gsub('.*[a-zA-Z]','',series_all$yck_seriesid)
  series_all<-series_all%>% group_by(yck_brandid) %>% summarise(maxs=max(yck_seriesid)) %>% as.data.frame()
  series_add<-left_join(series_add,series_all,by=c('yck_brandid'))
  series_add$maxs[is.na(series_add$maxs)]<-0
  
  series_add<-series_add %>% group_by(yck_brandid) %>% mutate(yck_seriesid=paste0(yck_brandid,'S',as.integer(maxs)+as.integer(factor(series_name)))) %>%
    as.data.frame() %>% dplyr::select(series_id,series_group_name,series_name,yck_seriesid) %>%
    mutate(is_import=is_import) %>% mutate(car_level='无',is_green='无')
  write.csv(series_add,paste0(local_file,"\\series_add.csv",sep=""),row.names = F,fileEncoding = "UTF-8",quote = F)
  loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
  dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE ","'",paste0(local_file,"/series_add.csv",sep=""),"'",
                                 " INTO TABLE config_vdatabase_yck_series CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;"))
  dbDisconnect(loc_channel)
  loc_channel<-dbConnect(MySQL(),user = local_defin_yun$user,host=local_defin_yun$host,password= local_defin_yun$password,dbname=local_defin_yun$dbname)
  dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE ","'",paste0(local_file,"/series_add.csv",sep=""),"'",
                                 " INTO TABLE config_vdatabase_yck_series CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;"))
  dbDisconnect(loc_channel)
}

####--车型库构建：车型ID增加--####
loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
dbSendQuery(loc_channel,'SET NAMES gbk')
model_add<-dbFetch(dbSendQuery(loc_channel,"SELECT a.model_id,a.Initial,a.brandid,d.yck_brandid,a.series_id,c.yck_seriesid FROM config_che300_major_info a 
        LEFT JOIN config_vdatabase_yck_model b ON a.model_id=b.model_id
        LEFT JOIN config_vdatabase_yck_series c ON a.series_id=c.series_id
        LEFT JOIN config_vdatabase_yck_brand d ON a.brandid=d.brandid
        WHERE yck_modelid IS NULL;"),-1)
model_all<-dbFetch(dbSendQuery(loc_channel,"SELECT yck_seriesid,yck_modelid FROM config_vdatabase_yck_model a
        INNER JOIN config_che300_major_info b ON a.model_id=b.model_id
        INNER JOIN config_vdatabase_yck_series c ON b.series_id=c.series_id;"),-1)
dbDisconnect(loc_channel)
if(nrow(model_add)>0){
  model_all$yck_modelid<-gsub('.*[a-zA-Z]','',model_all$yck_modelid)
  model_all<-model_all%>% group_by(yck_seriesid) %>% summarise(maxs=max(yck_modelid)) %>% as.data.frame()
  model_add<-left_join(model_add,model_all,by=c('yck_seriesid'))
  model_add$maxs[is.na(model_add$maxs)]<-0
  
  model_add<-model_add %>% group_by(yck_seriesid) %>% mutate(yck_modelid=paste0(yck_seriesid,'M',as.integer(maxs)+as.integer(factor(model_id)))) %>%
    as.data.frame() %>% dplyr::select(yck_modelid,model_id)
  write.csv(model_add,paste0(local_file,"\\model_add.csv",sep=""),row.names = F,fileEncoding = "UTF-8",quote = F)
  loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
  dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE ","'",paste0(local_file,"/model_add.csv",sep=""),"'",
                                 " INTO TABLE config_vdatabase_yck_model CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;"))
  dbDisconnect(loc_channel)
  loc_channel<-dbConnect(MySQL(),user = local_defin_yun$user,host=local_defin_yun$host,password= local_defin_yun$password,dbname=local_defin_yun$dbname)
  dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE ","'",paste0(local_file,"/model_add.csv",sep=""),"'",
                                 " INTO TABLE config_vdatabase_yck_model CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;"))
  dbDisconnect(loc_channel)
}

####--车型库构建：车型库信息更新--####
loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
dbSendQuery(loc_channel,'SET NAMES gbk')
dbSendQuery(loc_channel,"INSERT INTO config_vdatabase_yck_major_info SELECT a.yck_modelid,a.model_id,b.Initial,
                  b.brandid,c.yck_brandid,c.brand_name,d.series_group_name,b.series_id,d.yck_seriesid,d.series_name,
                  b.model_name,b.model_price,b.model_year,b.auto,b.liter,b.discharge_standard,b.max_reg_year,
                  b.min_reg_year,d.car_level,b.seat_number,b.hl_configs,b.hl_configc,d.is_green,d.is_import
                FROM config_vdatabase_yck_model a
                INNER JOIN config_che300_major_info b ON a.model_id=b.model_id
                INNER JOIN config_vdatabase_yck_brand c ON b.brandid=c.brandid
                INNER JOIN config_vdatabase_yck_series d ON b.series_id=d.series_id
            WHERE d.is_green !='无' AND d.car_level!='无' AND b.auto in('手动','自动','电动') 
            ON DUPLICATE KEY UPDATE config_vdatabase_yck_major_info.model_id=a.model_id")
dbSendQuery(loc_channel,"UPDATE config_vdatabase_yck_major_info SET is_green=1 WHERE model_id in(1127679,1127680,1132874,
    1132875,1132876,1132877,1132878,1134648,1134649,1143368,1143792,1145933,1145934,1145935,1145936,
    1145937,1145938,1145939,1146014,1151333,15587,15600,28772,29125,29126,30213,31862,31863,32496,32742)")
dbDisconnect(loc_channel)

####--车型库构建：车型库信息更新--####
loc_channel<-dbConnect(MySQL(),user = local_defin_yun$user,host=local_defin_yun$host,password= local_defin_yun$password,dbname=local_defin_yun$dbname)
dbSendQuery(loc_channel,'SET NAMES gbk')
dbSendQuery(loc_channel,"INSERT INTO config_vdatabase_yck_major_info SELECT a.yck_modelid,a.model_id,b.Initial,
                  b.brandid,c.yck_brandid,c.brand_name,d.series_group_name,b.series_id,d.yck_seriesid,d.series_name,
                  b.model_name,b.model_price,b.model_year,b.auto,b.liter,b.discharge_standard,b.max_reg_year,
                  b.min_reg_year,d.car_level,b.seat_number,b.hl_configs,b.hl_configc,d.is_green,d.is_import
                FROM config_vdatabase_yck_model a
                INNER JOIN config_che300_major_info b ON a.model_id=b.model_id
                INNER JOIN config_vdatabase_yck_brand c ON b.brandid=c.brandid
                INNER JOIN config_vdatabase_yck_series d ON b.series_id=d.series_id
            WHERE d.is_green !='无' AND d.car_level!='无' AND b.auto in('手动','自动','电动') 
            ON DUPLICATE KEY UPDATE config_vdatabase_yck_major_info.model_id=a.model_id")
dbSendQuery(loc_channel,"UPDATE config_vdatabase_yck_major_info SET is_green=1 WHERE model_id in(1127679,1127680,1132874,
    1132875,1132876,1132877,1132878,1134648,1134649,1143368,1143792,1145933,1145934,1145935,1145936,
    1145937,1145938,1145939,1146014,1151333,15587,15600,28772,29125,29126,30213,31862,31863,32496,32742)")
dbDisconnect(loc_channel)