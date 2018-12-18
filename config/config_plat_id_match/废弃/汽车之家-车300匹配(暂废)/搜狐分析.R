#清除缓存
rm(list = ls(all=T))
gc()
library(dplyr)
library(stringr)
library(raster)
library(ggplot2)
#help(package="dplyr")
#读取数据
library(RMySQL)
loc_channel<-dbConnect(MySQL(),user = "root",host="192.168.0.111",password= "000000",dbname="yck-data-center")
dbSendQuery(loc_channel,'SET NAMES gbk')
table.name<-dbListTables(loc_channel)
# field.name<-dbListFields(loc_channel,"")
yck_czb<-dbFetch(dbSendQuery(loc_channel,"SELECT * FROM spider_salesnum_souhu;"),-1)
dbDisconnect(loc_channel)
yck_czb<-yck_czb[,c(3,5,6,7)]

linshi<-yck_czb%>%filter(brand_name=='奥迪',series_name=='A4L')
ggplot(data=linshi,aes(as.Date(stat_date),salesNum,
                              group=cut.Date(as.Date(linshi$stat_date),breaks = "year"),
                              color=cut.Date(as.Date(linshi$stat_date),breaks = "year")))+
  geom_line()+
  geom_point()+
  facet_wrap(~series_name,nrow=4)
