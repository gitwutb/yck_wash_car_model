##############增量数据：此程序为优车库匹配汽车之家详细配置文件##########
rm(list = ls(all=T))
gc()
library(RODBC)
library(reshape2)
library(dplyr)
library(ggplot2)
library(RMySQL)
library(stringr)
library(e1071) 
library(tcltk)
library(lubridate)
#######################第一步：获取匹配信息###############
file_path<-gsub("\\/bat|\\/main|\\/Model_library.*","",tryCatch(dirname(rstudioapi::getActiveDocumentContext()$path),error=function(e){getwd()}))
loc_channel<-dbConnect(MySQL(),user = "root",host="192.168.0.111",password= "000000",dbname="yck-data-center")
dbSendQuery(loc_channel,'SET NAMES gbk')
yck1<-dbFetch(dbSendQuery(loc_channel,"select model_id autohome_id,brand_letter mark,brand_name brand,series_name series,`status` is_selling,CONCAT(series_name,' ',model_name) type_name,
                          FORMAT(model_price/10000,2) recommend_price,model_year year,series_group_name factory_name,'' produced_place FROM
                          config_autohome_major_info_tmp b;"),-1)
yck2<-dbFetch(dbSendQuery(loc_channel,"SELECT * FROM config_autohome_detail_info;"),-1)%>%dplyr::select(-url)
dbDisconnect(loc_channel)

loc_channel<-dbConnect(MySQL(),user = "data",host="youcku.com",password= "6wrzfhG",dbname="yck")
dbSendQuery(loc_channel,'SET NAMES gbk')
yck_have<-dbFetch(dbSendQuery(loc_channel,"SELECT DISTINCT autohome_id FROM yck_car_basic_config WHERE autohome_id is NOT NULL;"),-1)
id_max<-dbFetch(dbSendQuery(loc_channel,"SELECT MAX(id) FROM yck_car_basic_config;"),-1)%>%as.numeric()
dbDisconnect(loc_channel)
yck_not_have<-data.frame(autohome_id=setdiff(yck1$autohome_id,yck_have$autohome_id))
yck1<-merge(yck1,yck_not_have,by="autohome_id")
yck2<-merge(yck2,yck_not_have,by="autohome_id")

names(yck2)<-c("autohome_id","emission","level","engine","gear_box","length","width","height","body_structure",
              "wheelbase","weight","trunk_volume","intake","cylinder_arrangement","cylinders",
              "admission_gear","max_ps","max_n-m","fuel","fuel_grade","fuel_supply_system",
              "environmental_standards_org","drive_mode","front_suspension_type","rear_suspension_type",
              "power_type","car_boty_type","front_brake_type","rear_brake_type","parking_brake_type",
              "front_tire_specifications","rear_tire_specifications","master_air_bag","sub_air_bag",
              "front_side_air_bag","rear_side_air_bag","front_head_air_bag","rear_head_air_bag",
              "tire_pressure_monitoring","ISOFIX_child_seat_interfaces","car_internal_lock",
              "keyless_start_system","abs","brake_assist","stability_control","variable_suspension",
              "electric_roof","panoramic_roof","multifunction_steering_wheel","cruise","front_parking_radar",
              "rear_parking_radar","reverse_video_image","seat_material","electric_seat_memory",
              "front_seat_heating","rear_seat_heating","front_seat_ventilation","rear_seat_ventilation",
              "gps","low_beam_lamp","daytime_running_light","automatic_headlights","front_fog_lamp",
              "front_electric_window","rear_electric_window","mirror_electric_adjustment","mirror_heating",
              "air_conditioning_control_mode","color_outside","color_outside_code")
yck2$emission<-gsub(" .*","",yck2$emission)
yck2$length<-substr(yck2$length,1,4)
yck2$width<-substr(yck2$width,6,9)
yck2$height<-substr(yck2$height,11,14)
###c(33,35,37,51,56,58,65)后半段去掉
linshi<-c(33,35,37,51,56,58,65)
for (i in 1:length(linshi)) {
   yck2[,linshi[i]]<-gsub("\\?\\/.*|\\/.*","",yck2[,linshi[i]])
}
###c(34,36,38,52,57,59,66)前半段去掉
linshi<-c(34,36,38,52,57,59,66)
for (i in 1:length(linshi)) {
  yck2[,linshi[i]]<-gsub(".*\\/\\?|.*\\/","",yck2[,linshi[i]])
}
######字段归一化
for (i in 33:68) {
  yck2[,i]<-gsub("前标配|后标配|主标配|副标配|标配","Y",yck2[,i])
  yck2[,i]<-gsub("前选配|后选配|主选配|副选配|选配","-",yck2[,i])
  yck2[,i]<-gsub("前无|后无|主无|副无|无","N",yck2[,i])
}

##############################第三步：组合为最终输出##########################
yck1$autohome_id<-as.integer(yck1$autohome_id)
yck<-inner_join(yck1,yck2,by="autohome_id")
yck[is.na(yck)]<-'-'
yck$color_outside<-gsub("无","",yck$color_outside)
yck$color_outside_code<-gsub("无","",yck$color_outside_code)
yck$color_outside<-substr(yck$color_outside,1,(nchar(as.character(yck$color_outside))-1))
yck$color_outside_code<-substr(yck$color_outside_code,1,(nchar(as.character(yck$color_outside_code))-1))
yck<-data.frame(id=c((id_max+1):(id_max+nrow(yck))),yck,environmental_standards=yck$environmental_standards_org)
yck$environmental_standards<-yck$environmental_standards_org
yck$environmental_standards<-gsub("\\+OBD|\\)","",yck$environmental_standards)
yck$environmental_standards<-gsub("3|III|Ⅲ","三",yck$environmental_standards)
yck$environmental_standards<-gsub("2|Ⅱ|II","二",yck$environmental_standards)
yck$environmental_standards<-gsub("4|IV|Ⅳ","四",yck$environmental_standards)
yck$environmental_standards<-gsub("6|VI|Ⅵ","六",yck$environmental_standards)
yck$environmental_standards<-gsub("5|V","五",yck$environmental_standards)
yck$environmental_standards<-gsub("\\(","\\/",yck$environmental_standards)

write.csv(yck,paste0(file_path,"/Model_library/Model_library_autohome/yck_detail.csv"),
          row.names = F,fileEncoding = "UTF-8",quote = F)
loc_channel<-dbConnect(MySQL(),user = "root",host="192.168.0.111",password= "000000",dbname="yck")
dbSendQuery(loc_channel,'SET NAMES gbk')
dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE ","'",paste0(file_path,"/Model_library/Model_library_autohome/yck_detail.csv"),"'",
                               " INTO TABLE yck_car_basic_config CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;"))
dbDisconnect(loc_channel)

########*****************############***********##########
##########第二部分：定期处理生产系统中车型库颜色字段缺失#######
loc_channel<-dbConnect(MySQL(),user = "root",host="192.168.0.111",password= "000000",dbname="yck")
dbSendQuery(loc_channel,'SET NAMES gbk')
dbSendQuery(loc_channel,"UPDATE yck.yck_car_basic_config a,`yck-data-center`.config_autohome_detail_info b
  SET a.color_outside=REPLACE(CONCAT(b.color_outside,';'),';;',''),a.color_outside_code=REPLACE(CONCAT(b.color_outside_code,';'),';;','')
                          WHERE a.autohome_id=b.autohome_id AND a.color_outside='' AND b.color_outside!='无'")
bf_yck<-dbFetch(dbSendQuery(loc_channel,"SELECT * FROM yck_car_basic_config;"),-1)
dbDisconnect(loc_channel)
bf_yck$autohome_id[which(is.na(bf_yck$autohome_id))]<-''
write.csv(bf_yck,paste0(file_path,"/Model_library/Model_library_autohome/bf_yck.csv"),
          row.names = F,fileEncoding = "UTF-8",quote = F)

loc_channel<-dbConnect(MySQL(),user = "data",host="youcku.com",password= "6wrzfhG",dbname="yck")
dbSendQuery(loc_channel,'SET NAMES gbk')
#dbSendQuery(loc_channel,"TRUNCATE TABLE yck_car_basic_config")
dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE ","'",paste0(file_path,"/Model_library/Model_library_autohome/bf_yck.csv"),"'",
                               "REPLACE INTO TABLE yck_car_basic_config CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;"))
dbSendQuery(loc_channel,"UPDATE yck_car_basic_config SET autohome_id=NULL WHERE autohome_id=0")
dbDisconnect(loc_channel)

loc_channel<-dbConnect(MySQL(),user = "data",host="120.79.98.108",password= "543asdfQ",dbname="yck")
dbSendQuery(loc_channel,'SET NAMES gbk')
#dbSendQuery(loc_channel,"TRUNCATE TABLE yck_car_basic_config")
dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE ","'",paste0(file_path,"/Model_library/Model_library_autohome/bf_yck.csv"),"'",
                               "REPLACE INTO TABLE yck_car_basic_config CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;"))
dbSendQuery(loc_channel,"UPDATE yck_car_basic_config SET autohome_id=NULL WHERE autohome_id=0")
dbDisconnect(loc_channel)



# ####第三部分：异常情况处理，出错行数据重新找回
# #######################第一步：获取匹配信息###############
# file_path<-c("E:/Work_table/gitwutb/git_project/yck_wash_car_model/Model_library/Model_library_autohome")
# loc_channel<-dbConnect(MySQL(),user = "root",host="192.168.0.111",password= "000000",dbname="yck-data-center")
# dbSendQuery(loc_channel,'SET NAMES gbk')
# yck1<-dbFetch(dbSendQuery(loc_channel,"select model_id autohome_id,brand_letter mark,brand_name brand,series_name series,`status` is_selling,CONCAT(series_name,' ',model_name) type_name,
#                           FORMAT(model_price/10000,2) recommend_price,model_year year,series_group_name factory_name,'' produced_place FROM
#                           config_autohome_major_info_tmp b;"),-1)
# yck2<-dbFetch(dbSendQuery(loc_channel,"SELECT * FROM config_autohome_detail_info;"),-1)%>%dplyr::select(-url)
# dbDisconnect(loc_channel)
# 
# loc_channel<-dbConnect(MySQL(),user = "root",host="192.168.0.111",password= "000000",dbname="yck")
# dbSendQuery(loc_channel,'SET NAMES gbk')
# yck_have<-dbFetch(dbSendQuery(loc_channel,"SELECT id,autohome_id FROM yck_car_basic_config WHERE environmental_standards REGEXP '#' OR environmental_standards =''
#  OR environmental_standards ='富士白' AND autohome_id IS NOT NULL;"),-1)
# dbDisconnect(loc_channel)
# yck1<-merge(yck_have,yck1,by="autohome_id")%>%dplyr::select(-id)
# yck2<-merge(yck2,yck_have,by="autohome_id")%>%dplyr::select(-id)
# 
# yck<-inner_join(yck_have,yck,by="autohome_id")