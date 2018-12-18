##############################################################
############---------函数fun_match每次减少某一个字段----------
fun_match<-function(qx_data_input,qx_che300,a){
  data_input<-qx_data_input[,3]
  che300<-qx_che300[,3]
  for (j in a) {
    data_input<-paste(data_input,qx_data_input[,j],sep = "")
    che300<-paste(che300,qx_che300[,j],sep = "")
  }
  data_input<-gsub(" ","",data_input)
  che300<-gsub(" ","",che300)
  data_input_match<-unique(data.frame(x=data_input,id_data_input=qx_data_input$X,rname=qx_data_input$car_model_name))
  che300_match<-unique(data.frame(x=che300,id_che300=qx_che300$car_id,cname=qx_che300$car_model_name))
  data_input_match$x<-as.character(data_input_match$x)
  che300_match$x<-as.character(che300_match$x)
  #匹配
  inner_join(data_input_match,che300_match,by="x")
}
