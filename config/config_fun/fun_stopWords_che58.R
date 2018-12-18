#####################用于清洗名称中不规范词汇（统一命名规则）######################
fun_stopWords_che58<-function(data_input,qx_name){
  ###--------清洗奔驰------
  car_series1<-gsub("GRAND EDITION","特别版",car_series1)
  car_series1<-gsub("改装房车","",car_series1)
  qx_name[grep("奔驰",car_series1)]<-gsub("AMG","",qx_name[grep("奔驰",car_series1)])
  benchi<-str_extract(car_series1,"荣威.*|福特(E|F).*|捷豹.*|奥迪TTS|奥迪TT|奥迪(A8|RS5)|奥迪S[1-9]|奔驰.*")
  benchi<-gsub("荣威|福特E|福特|捷豹|奥迪|奔驰|(级| |-).*","",benchi)
  benchi[-grep("",benchi)]<-""
  #清除e L等
  forFun<-function(i){
    sub(benchi[i],"",qx_name[i])
  }
  qx_name<-unlist(lapply(1:length(car_series1),forFun))
  ##获取奔驰数字串
  linshi<-str_extract(car_series1,"奔驰.*")
  linshi[grep("奔驰",car_series1)]<-str_extract(qx_name[grep("奔驰",car_series1)],"[0-9]{3}|[0-9]{2}")
  linshi[-grep("",linshi)]<-""
  benchi<-paste(benchi,linshi,sep = "")
  #清除数字
  forFun<-function(i){
    sub(linshi[i],"",qx_name[i])
  }
  qx_name<-unlist(lapply(1:length(car_series1),forFun))
  car_series1[grep("奔驰",car_series1)]<-gsub("级","",car_series1[grep("奔驰",car_series1)])
  
  ##清洗规则
  qx_name<-gsub("型|舱|款|版","型",qx_name)
  qx_name<-gsub("FR\\+","FR佳",qx_name)
  qx_name<-gsub("FRESH(-|\\+)ALLOY","天地",qx_name)
  qx_name<-gsub("世博型|世博","贺岁",qx_name)
  qx_name<-gsub("个性","限量",qx_name)
  qx_name<-gsub("新白金","白金",qx_name)
  qx_name<-gsub("超值物流","傲运通",qx_name)
  qx_name<-gsub("(一|二|两|三|四|五|十|[0-9]+)周年","周年",qx_name)
  qx_name<-gsub("周年纪念","纪念",qx_name)
  qx_name<-gsub("不带OBD|毂|16.4|5阀|带PEP|无侧拉门|酷黑骑士|油改气|单增压|节能|系列","",qx_name)
  qx_name<-gsub("带空调|空调型","空调",qx_name)
  qx_name<-gsub("Féline","Feline",qx_name)
  qx_name<-gsub("M/T","MT",qx_name)
  qx_name<-gsub("A/T","AT",qx_name)
  qx_name<-gsub("4驱|AWD|4WD-4|4WD|4X4|4×4|4\\*4","四驱",qx_name)
  qx_name<-gsub("2WD|4X2|4×2|4\\*2","两驱",qx_name)
  qx_name<-gsub("1.8T/5V","1.8T",qx_name)
  qx_name<-gsub("HYBRID.*(新|)混合动力|(新|)混合动力.*HYBRID|HYBRID|混合动力","混动",qx_name)
  qx_name<-gsub("MHD","微型混动",qx_name)
  qx_name<-gsub("钛金内饰版|象牙内饰版","型",qx_name)
  qx_name<-gsub("马力|公里","KW",qx_name)
  qx_name<-gsub("新一代|(第|)(Ⅰ|一|I|1)(代|型)","一代",qx_name)
  qx_name<-gsub("(第|)(Ⅱ|二|2)(代|型)","二代",qx_name)
  qx_name<-gsub("(第|)(Ⅲ|三|3)(代|型)","三代",qx_name)
  qx_name<-gsub("(第|)(Ⅳ|四|4)(代|型)","四代",qx_name)
  qx_name<-gsub("(第|)(五|5)(代|型)","五代",qx_name)
  qx_name<-gsub("LSTD","加长型 标准型",qx_name)
  qx_name<-gsub("GSTD","加高型 标准型",qx_name)
  qx_name<-gsub("LDLX","加长型 豪华型",qx_name)
  qx_name<-gsub("GDLX","加高型 豪华型",qx_name)
  qx_name[grep("STD.*标准|标准.*STD",qx_name)]<-gsub("STD1|STD2|STD","",qx_name[grep("STD.*标准|标准.*STD",qx_name)])
  qx_name[grep("^MG6",car_series1)]<-gsub("两厢","掀背",qx_name[grep("^MG6",car_series1)])
  car_series1[grep("索纳塔",car_series1)][grep("2011",car_year[grep("索纳塔",car_series1)])]<-"索纳塔八"
  qx_name<-gsub("STD(\\(.*\\)|X|)","标准型",qx_name)
  qx_name<-gsub("DLX(\\(.*\\)|)","豪华型",qx_name)
  qx_name<-gsub("(硬|软)顶(型|)","",qx_name)
  qx_name<-gsub("(敞篷|敞蓬)(型|车|)","敞篷",qx_name)
  qx_name<-gsub("商用车","商务车",qx_name)
  qx_name<-gsub("型.*爵士黑","爵士型",qx_name)
  qx_name<-gsub("(双门|)轿跑(车|型|)","COUPE",qx_name)
  qx_name<-gsub("(双门|)跑车(型|)","COUPE",qx_name)
  qx_name<-gsub("柴油VE泵"," VE泵柴油",qx_name)
  qx_name<-gsub("柴油共轨","共轨柴油",qx_name)
  qx_name<-gsub("\\+助力转向|助力转向|(转向|加装|有|带| )助力","助力",qx_name)
  qx_name<-gsub("双燃料(型|车)","双燃料",qx_name)
  qx_name<-gsub("单燃料(型|车)","单燃料",qx_name)
  qx_name<-gsub("液化石油气","LPG",qx_name)
  qx_name<-gsub("压缩天然气","CNG",qx_name)
  qx_name<-gsub("液化天然气","LNG",qx_name)
  qx_name<-gsub("厢货车|厢式货车","厢货",qx_name)
  qx_name<-gsub("箱","厢",qx_name)
  qx_name<-gsub("变速厢","变速箱",qx_name)
  qx_name<-gsub("厢式车|厢车","厢式",qx_name)
  qx_name<-gsub("货厢","厢",qx_name)
  qx_name<-gsub("标准厢","标厢",qx_name)
  qx_name<-gsub("仓栏车|仓栅车","仓栅",qx_name)
  qx_name<-gsub("加长车|加长型","加长",qx_name)
  qx_name<-gsub("长车身|长型","长车",qx_name)
  qx_name<-gsub("短车身|短型","短车",qx_name)
  qx_name<-gsub("天窗型车身|(全景|带|新)天窗(型|)","天窗",qx_name)
  qx_name<-gsub("真皮座椅|真皮型","真皮",qx_name)
  qx_name<-gsub("织物座椅|织物型","织物",qx_name)
  qx_name<-gsub("带.*GO.*功能","",qx_name)
  qx_name<-gsub("铝合金轮","铝轮",qx_name)
  qx_name<-gsub("BLACKORANGE","墨橘",qx_name)
  qx_name<-gsub("精典","经典",qx_name)
  qx_name<-gsub("经济配置","经济",qx_name)
  qx_name<-gsub("经典特别","特别",qx_name)
  qx_name<-gsub("物流型","物流车",qx_name)
  qx_name<-gsub("欧风型|欧洲型|欧规型|欧规|欧风|欧型","欧洲",qx_name)
  qx_name[grep("蓝瑟|卡罗拉|风云2",car_series1)]<-gsub("掀背|特装|卓越","",qx_name[grep("蓝瑟|卡罗拉|风云2",car_series1)])
  qx_name[grep("名图",car_series1)]<-gsub("豪华","",qx_name[grep("名图",car_series1)])
  qx_name<-gsub("F-150","F150",qx_name)
  qx_name<-gsub("F-350","F350",qx_name)
  qx_name<-gsub("F-450","F550",qx_name)
  qx_name<-gsub("蛇行","蛇形",qx_name)
  
  #######-----------正则排量标准-------
  car_OBD<-str_extract(qx_name,"OBD")
  car_OBD[which(is.na(car_OBD))]<-""
  car_discharge_standard<-str_extract(qx_name,"(国|欧)(Ⅱ|Ⅲ|Ⅳ|III|II|IV|VI|Ⅵ|V|二|三|四|五|2|3|4|5)(型|)|京(5|五|V)")
  car_discharge_standard<-gsub("3|III|Ⅲ","三",car_discharge_standard)
  car_discharge_standard<-gsub("2|Ⅱ|II","二",car_discharge_standard)
  car_discharge_standard<-gsub("4|IV|Ⅳ","四",car_discharge_standard)
  car_discharge_standard<-gsub("6|VI|Ⅵ","六",car_discharge_standard)
  car_discharge_standard<-gsub("5|V","五",car_discharge_standard)
  qx_name<-gsub("(国|欧)(Ⅱ|Ⅲ|Ⅳ|III|II|IV|VI|Ⅵ|V|二|三|四|五|2|3|4|5)(型|)|京(5|五|V)","",qx_name)

  
  ##填写的排量标准----------------------------------------------------------------------------------------------------
  car_discharge<-data_input$discharge_standard
  car_discharge<-gsub("1","一",car_discharge)
  car_discharge<-gsub("2","二",car_discharge)
  car_discharge<-gsub("3","三",car_discharge)
  car_discharge<-gsub("4","四",car_discharge)
  car_discharge<-gsub("5","五",car_discharge)
  car_discharge<-gsub("6","六",car_discharge)
  car_discharge<-gsub("-|无数据","",car_discharge)
  
  ###------------改款等---------#########
  qx_name<-gsub("(独立|侧翻)座椅|带侧拉门|新型|改型(经典|双擎|E|)|双擎|\\(\\+\\)|冠军型|重装型|(08)升级型|换代|改装型|PLUS(型|)|[0-9]系|通用GMC|公爵|御系列","改版",qx_name)
  car_restyle<-trim(str_extract(qx_name,"升级型|改版"))
  linshi<-str_extract(qx_name,"(一|二|三|四|五)代")
  linshi[which(is.na(linshi))]<-""
  car_restyle[which(is.na(car_restyle))]<-""
  car_restyle<-paste(car_restyle,linshi,sep="")
  qx_name<-gsub("\\(北京型\\)|升级型|改版|(一|二|三|四|五)代","",qx_name)
  
  ####--------------高中平低顶----------
  car_height_t<-str_extract(qx_name,"(半高|标准|高|中低|超低|中|平|低)顶|([0-9][.][0-9]|[0-9]+)米|[0-9]{4}长")
  qx_name<-gsub("(半高|标准|高|中低|超低|中|平|低)顶|([0-9][.][0-9]|[0-9]+)米|[0-9]{4}长","",qx_name)
  
  ######################-----------人人车中轴距没有数据------------------------------########
  #轴距
  car_wheelbase<-str_extract(qx_name,"轴距[0-9]{4}|[0-9]{4}轴距|加长轴加长轴|(加长|标准|中短|超长|中短|长|中|短)轴|轴距")
  qx_name<-gsub("轴距[0-9]{4}|[0-9]{4}轴距|加长轴加长轴|(加长|标准|中短|超长|中短|长|中|短)轴|轴距","",qx_name)
  
  
  ####------------------燃料类型---------------
  car_oil<-str_extract(qx_name,"(三菱|丰田4Y长城)发动机|(丰田4Y|)绵阳|全柴|汽油|(2|莱动|VE泵|高压共轨|共轨|)柴油|混动|微型混动|双燃料|单燃料")
  linshi<-str_extract(qx_name,"LPG|LNG|CNG")
  linshi[which(is.na(linshi))]<-""
  car_oil[which(is.na(car_oil))]<-""
  car_oil<-paste(car_oil,linshi,sep="")
  qx_name<-gsub("混动(型|车|)|(493|491|2| |莱动|VE泵|高压共轨|共轨|)柴油(车型|发动机|型|机|)|汽油(车型|发动机|型|机|)|(微型|E驱)混动|(LPG|LNG|CNG)(车型|)|双燃料|491发动机|全柴|(丰田4Y|)(绵阳|长城)(发动机|)|三菱发动机"," ",qx_name)
  #----是否原装及进口-----
  linshi<-str_extract(qx_name,"节油Π|节油|平行进口|国机|K机|(组装|原装|进口)")
  linshi[which(is.na(linshi))]<-""
  car_oil<-paste(car_oil,linshi,sep="")
  qx_name<-gsub("智能节油|节油Π|节油|平行进口|国机|K机|(组装|原装|进口)(发动机|机|)"," ",qx_name)
  
  
  ####--------车座位--------------真皮-丝绒--天窗---
  qx_name<-gsub("人座","座",qx_name)
  qx_name<-gsub("七座","7座",qx_name)
  qx_name<-gsub("六座","6座",qx_name)
  qx_name<-gsub("五座","5座",qx_name)
  qx_name<-gsub("两座","2座",qx_name)
  car_site<-str_extract(qx_name,"([0-9]+-|-|)[0-9]+(\\/[0-9]+\\/[0-9]+|\\/[0-9]+|)座|9\\+座|(2|4|5|7)人")
  qx_name<-gsub("([0-9]+-|-|)[0-9]+(\\/[0-9]+\\/[0-9]+|\\/[0-9]+|)(座型|座)|9\\+(座型|座)|(2|4|5|7)人","",qx_name)
  car_site<-gsub("^-|\\+","",car_site)
  car_site<-gsub("-","/",car_site)
  car_site<-gsub("人","座",car_site)
  ##真皮-丝绒
  linshi<-str_extract(qx_name,"真皮|丝绒|织物")
  linshi[grep("",linshi)]<-linshi[grep("",linshi)]
  linshi[which(is.na(linshi))]<-""
  car_site[which(is.na(car_site))]<-""
  car_site<-paste(car_site,linshi,sep="")
  #天窗
  linshi<-str_extract(qx_name,"天窗经典|双天窗|天窗")
  linshi[grep("",linshi)]<-str_c(linshi[grep("",linshi)],"型",sep="")
  linshi[which(is.na(linshi))]<-""
  car_site[which(is.na(car_site))]<-""
  car_site<-paste(car_site,linshi,sep="")
  #空调
  linshi<-str_extract(qx_name,"空调")
  linshi[grep("",linshi)]<-str_c(linshi[grep("",linshi)],"型",sep="")
  linshi[which(is.na(linshi))]<-""
  car_site[which(is.na(car_site))]<-""
  car_site<-paste(car_site,linshi,sep="")
  #纪念
  linshi<-str_extract(qx_name,"纪念")
  linshi[grep("",linshi)]<-str_c(linshi[grep("",linshi)],"型",sep="")
  linshi[which(is.na(linshi))]<-""
  car_site[which(is.na(car_site))]<-""
  car_site<-paste(car_site,linshi,sep="")
  qx_name<-gsub("真皮|丝绒|织物|天窗经典|双天窗|天窗|空调|纪念"," ",qx_name)
  
  
  
  #######----------car_series3提炼车型-------
  car_series3<-str_extract(qx_name,c(str_c(rm_rule$qx_series3,sep="",collapse = "|")))
  qx_name<-gsub(c(str_c(rm_rule$qx_series3,sep="",collapse = "|")),"",qx_name)
  linshi<-str_extract(qx_name,"掀背")
  linshi[which(is.na(linshi))]<-""
  car_series3[which(is.na(car_series3))]<-""
  car_series3<-paste(car_series3,linshi,sep="")
  
  #NAVI和导航型清洗
  qx_name[grep("(导航(型|).*)NAVI|(NAVI(.*导航(型|)))",qx_name)]<-
    gsub("导航(型|)","",qx_name[grep("(导航(型|).*)NAVI|(NAVI(.*导航(型|)))",qx_name)])
  car_series3[grep("(-|)NAVI(型|-|)",qx_name)]<-"导航型"
  qx_name[grep("(-|)NAVI(型|-|)",qx_name)]<-gsub("(-|)NAVI(型|-|)"," ",qx_name[grep("(-|)NAVI(型|-|)",qx_name)])
  linshi<-str_extract(qx_name,"(特睿|影音|音乐|电子|语音|北斗|)导航(爵士|ESP|)")
  linshi[grep("",linshi)]<-str_c(linshi[grep("",linshi)],"型",sep="")
  linshi[which(is.na(linshi))]<-""
  car_series3[which(is.na(car_series3))]<-""
  car_series3<-paste(car_series3,linshi,sep="")
  qx_name<-gsub("(特睿|影音|音乐|电子|(DVD|)语音|北斗|)导航(爵士|ESP|)(型|)|掀背(型|)"," ",qx_name)
  
  
  #马力（KW）car_hp
  car_hp<-str_extract(qx_name,"(([0-9]|)[0-9][.][0-9]|[0-9]+)KW|大KW(型|)")
  qx_name<-gsub("(([0-9]|)[0-9][.][0-9]|[0-9]+)KW|大KW(型|)","",qx_name)
  
  
  ##清除排量-国标car_displacement_standard--car_Displacement---car_Displacement_t(排量类型)---car_oil(燃油类型)---car_drive(几驱)
  ###---排量car_lter_t还需要清洗（命名规则统一）|T|L|i
  car_desc<-str_extract(qx_name,"[0-9][.][0-9](| )(TFSI|FSI|TSI|CRDI|TDDI)|[0-9][.][0-9](GTDI|(| )TDI|TGI|XV|GX|GV|GS|THP|TGDI|GDI|TCI|TID|GT|DIT|XT|TD|GI|HQ|HG|HV|TI|TC|VTEC|VVT-I|CVVT|DVVT|VVT)|[0-9]+(TGDI|TSI|THP|HP)|(TSI|THP|HP|GTDI)[0-9][0-9][0-9]|(GTDI|GDI|TFSI|FSI|TSI|SIDI|TCI |VTEC|VVT-I|CVVT|DVVT|VVT|MPI)|([0-9][0-9]( |)|)(TFSI|FSI|TDI)| ((18|20|25|28|40|50)T|30E|(15|25|36)S|15N|30H) ")
  #别克.*(XT|GT|LT|CT3|CT2|CT1|CT)  car_desc
  linshi<-str_extract(qx_name[grep("GL8|凯越",car_model_name)],"LE|LX|LS|XT|GT|LT|CT3|CT2|CT1|CT")
  linshi[which(is.na(linshi))]<-""
  car_desc[which(is.na(car_desc))]<-""
  car_desc[grep("GL8|凯越",car_model_name)]<-paste(car_desc[grep("GL8|凯越",car_model_name)],linshi,sep = "")
  qx_name[grep("GL8|凯越",car_model_name)]<-
    gsub("LE|LX|LS|XT|GT|LT|CT3|CT2|CT1|CT","",qx_name[grep("GL8|凯越",car_model_name)])
  car_liter_wu<-str_extract(qx_name,"[0-9][.][0-9](T|D|JS|JE|JC|J|SE|SL|SX|SI|SC|S|H|G|E|V|L|I|)")
  car_liter_wu[which(is.na(car_liter_wu))]<-""
  qx_name<-gsub("[0-9][.][0-9](| )(TFSI|FSI|TSI|CRDI|TDDI)|[0-9][.][0-9](GTDI|(| )TDI|TGI|XV|GX|GV|GS|TSI|THP|TGDI|GDI|TCI|TID|GT|DIT|XT|TD|GI|HQ|HG|HV|TI|TC|VTEC|VVT-I|CVVT|DVVT|VVT|T|D|SE|SL|SX|SI|SC|S|H|G|E|V|L|I)|[0-9]+(TGDI|TSI|THP|HP)|(TSI|THP|HP|GTDI)[0-9][0-9][0-9]|(GTDI|GDI|TFSI|FSI|TSI|SIDI|TCI |VTEC|VVT-I|CVVT|DVVT|VVT|MPI)|([0-9][0-9]( |)|)(TFSI|FSI|TDI)| ((18|20|25|28|40|50)T|30E|(15|25|36)S|15N|30H) |[0-9][.][0-9]"," ",qx_name)
  #提炼VTI-   car_desc
  car_desc[grep("TYPE-S|4MATIC|GLXI|VTI-L|VTI-E|VTI-S|LXI|EXI|VTI[0-9]+|VTI|GLX-I|GS-I|GL-I|G-I",qx_name)]<-
    str_extract(qx_name,"TYPE-S|4MATIC|GLXI|GLXI|VTI-L|VTI-E|VTI-S|LXI|EXI|VTI[0-9]+|VTI|GLX-I|GS-I|GL-I|G-I")[grep("TYPE-S|4MATIC|GLXI|GLXI|VTI-L|VTI-E|VTI-S|LXI|EXI|VTI[0-9]+|VTI|GLX-I|GS-I|GL-I|G-I",qx_name)]
  qx_name<-gsub("TYPE-S|4MATIC|GLXI|GLXI|VTI-L|VTI-E|VTI-S|LXI|EXI|VTI[0-9]+|VTI|GLX-I|GS-I|GL-I|G-I","",qx_name)
  ####对倒置的数据进行归一化
  car_desc<-gsub(" ","",car_desc)
  car_liter_wu[grep("[0-9][.][0-9].*",car_desc)]<-car_desc[grep("[0-9][.][0-9].*",car_desc)]
  car_desc<-gsub("[0-9][.][0-9].*| ","",car_desc)
  car_desc[which(is.na(car_desc))]<-""
  ce1<-str_extract(car_desc,"[0-9]+")
  ce1[which(is.na(ce1))]<-""
  car_desc<-gsub("[0-9]+","",car_desc)
  car_desc<-paste(ce1,car_desc,sep = "")
  
  
  ######-------发动机类型-----
  car_engine<-str_extract(qx_name,c(str_c(rm_rule$qx_engine,"(([A-Za-z0-9]|-)+|)",sep="",collapse = "|")))
  qx_name<-gsub(paste(rm_rule$qx_engine,"(([A-Za-z0-9]|-)+|)",sep="",collapse = "|"),"",qx_name)
  car_engine1<-str_extract(qx_name,c(str_c("H8D|HAD|H6B|H5B|H3B|GZ[0-9]{3}|B1A|B3BZ|B3XZ|B6B|B6BZ|BAD","(([A-Za-z0-9]|-)+|)",sep="",collapse = "|")))
  qx_name<-gsub(paste("H8D|HAD|H6B|H5B|H3B|GZ[0-9]{3}|B1A|B3BZ|B3XZ|B6B|B6BZ|BAD","(([A-Za-z0-9]|-)+|)",sep="",collapse = "|"),"",qx_name)
  
  
  
  ##汽车驱动类型
  qx_name<-gsub("前驱|后驱(型|)","两驱",qx_name)
  car_drive<-str_extract(qx_name,"两驱|四驱|全驱|(无|)助力")
  qx_name<-gsub("带OBD|OBD系列|\\+OBD|OBD"," ",qx_name)
  qx_name<-gsub("(全驱|两驱|(分时|全时|)四驱)(型|)|(有|无|)助力"," ",qx_name)
  
  
  ##----手自动清洗----(CVT|DSG|AT|MT|AMT|G-DCT|DCT)
  qx_name<-gsub("CVT无级变速|无级变速|自动.*CVT|CVT.*自动|-CVT|CVT","自动",qx_name)
  qx_name<-gsub("双离合.*手自动一体","自动",qx_name)
  qx_name<-gsub("手自一体型|手自动一体型|手自动一体|手自一体","自动",qx_name)
  qx_name<-gsub("DSG双离合|双离合器|双离合|G-DCT|DCT|DSG","自动双离合",qx_name)
  qx_name<-gsub("[^a-zA-Z0-9]AT|(-| )[0-9]AT|AT( |-)","自动",qx_name)
  qx_name[grep("AT[^a-zA-Z0-9]",qx_name)]<-gsub("AT","自动",qx_name[grep("AT[^a-zA-Z0-9]",qx_name)])
  qx_name<-gsub("AMT智能手动版|AMT智能手动|AMT|EMT|IMT"," 自动 ",qx_name)
  #手动
  qx_name<-gsub("^MT|[^a-zA-Z0-9]MT|MT(-| )","手动",qx_name)
  ##car_auto获取手自动
  car_auto<-data_input$auto
  car_auto<-str_extract(qx_name,"自动|G-DCT|[0-9]MT|手动")
  qx_name<-gsub("(自动|手动)(型|挡|档|变速)|自动|手动|[0-9](档|挡)|(4|5|6|六)速|[^a-zA-Z0-9]MT|(-| )[0-9]MT","",qx_name)
  
  #双离合
  linshi<-str_extract(qx_name,"双离合")
  linshi[which(is.na(linshi))]<-""
  car_hp[which(is.na(car_hp))]<-""
  car_hp<-paste(car_hp,linshi,sep="")
  qx_name<-gsub("双离合","",qx_name)
  
  ##car_door几门几款-------
  qx_name<-gsub("双门|二门","两门",qx_name)
  qx_name<-gsub("大双排","大双",qx_name)
  qx_name<-gsub("小双排","小双",qx_name)
  qx_name<-gsub("中双排","中双",qx_name)
  qx_name<-gsub("标双排","标双",qx_name)
  qx_name<-gsub("一排半","排半",qx_name)
  qx_name<-gsub("大单排","大单",qx_name)
  qx_name<-gsub("3门","三门",qx_name)
  qx_name<-gsub("4门","四门",qx_name)
  qx_name<-gsub("5门","五门",qx_name)
  car_door<-str_extract(qx_name,"两门|三门|四门|五门|(大|小|中)(双|单)|标双|(双|单)排|CROSS($| )|QUATTRO|排半")
  qx_name<-gsub("(两门|三门|四门|五门)(型|)|(大|小|中)(双|单)|标双|(铁|)(双|单)排|CROSS($| )|QUATTRO|排半|云100"," ",qx_name)
  
  ########%$^^^%%^%^清洗垃圾信息%……￥…………#######
  qx_name[-grep("型",qx_name)]<-paste0(qx_name[-grep("型",qx_name)],"型",sep="")
  linshi<-str_locate_all(qx_name,"型")
  linshi_wz<-sapply(linshi,length)
  wzfun<-function(i){
    linshi[[i]][linshi_wz[i]]
  }
  wz<-unlist(lapply(1:length(linshi), wzfun))
  qx_name<-str_sub(qx_name,1,wz)
  ##----####
  qx_name<-gsub("风行|XRV|广汽|哈弗M4|吉利EC7|吉利|吉普|JEEP|劲取|卡宴|旗云|日产|长安EADO|长安|长丰|制造|致胜","",qx_name)
  qx_name<-gsub(c(str_c(rm_che58$key,".*",sep="",collapse = "|")),"",qx_name)
  ########%$^^^%%^%^清洗垃圾信息%……￥…………#######
  
  
  #####------qx_name最后清洗----------
  qx_name<-gsub("\\/|--","-",qx_name)
  qx_name<-gsub(" -|- |^-|-$|\\(|\\)|型"," ",qx_name)
  qx_name<-trim(qx_name)
  qx_name<-gsub("转向"," ",qx_name)
  qx_name<-gsub("LUXURY","LUX",qx_name)
  ################qx_name<-gsub("XL-LUX|LUX","至尊",qx_name)
  qx_name<-gsub(" +"," ",qx_name)
  
  ############----------------测试qx_name----------
  qx_name1<-str_extract(qx_name,"(([\u4e00-\u9fa5]+ |)[\u4e00-\u9fa5]+ [\u4e00-\u9fa5]+)|[\u4e00-\u9fa5]+([A-Za-z0-9]+|)[\u4e00-\u9fa5]+|[\u4e00-\u9fa5]+|[\u4e00-\u9fa5]")
  qx_name1[which(is.na(qx_name1))]<-""
  forFun1<-function(i){
    sub(qx_name1[i],"",qx_name[i])
  }
  qx_name2<-unlist(lapply(1:length(qx_name1),forFun1))
  linshi<-str_extract(qx_name2,"[\u4e00-\u9fa5]+ [\u4e00-\u9fa5]+|[\u4e00-\u9fa5]+|[\u4e00-\u9fa5]")
  linshi[which(is.na(linshi))]<-""
  forFun<-function(i){
    sub(linshi[i],"",qx_name2[i])
  }
  qx_name2<-unlist(lapply(1:length(linshi),forFun))
  qx_name2<-gsub("\\/|--|\\+","-",qx_name2)
  qx_name2<-gsub(" -|- |^-|-$|GPS|\\(|\\)|型"," ",qx_name2)
  qx_name2<-trim(qx_name2)
  qx_name2<-gsub(" +"," ",qx_name2)
  qx_name1<-gsub(" ","",paste(qx_name1,linshi,sep=""))
  #######qx_name1顺序整理##########
  linshi<-str_extract(qx_name1,"炫装|荣耀|足金")
  qx_name1<-gsub("炫装|荣耀|足金","",qx_name1)
  linshi[which(is.na(linshi))]<-""
  qx_name1[which(is.na(qx_name1))]<-""
  qx_name1<-paste(qx_name1,linshi,sep="")
  linshi<-str_extract(qx_name1,"运动")
  qx_name1<-gsub("运动","",qx_name1)
  linshi[which(is.na(linshi))]<-""
  qx_name1[which(is.na(qx_name1))]<-""
  qx_name1<-paste(qx_name1,linshi,sep="")
  linshi<-str_extract(qx_name1,"白金|劲悦|罗兰加洛斯|周年|典藏")
  qx_name1<-gsub("白金|劲悦|罗兰加洛斯|周年|典藏","",qx_name1)
  linshi[which(is.na(linshi))]<-""
  qx_name1[which(is.na(qx_name1))]<-""
  qx_name1<-paste(qx_name1,linshi,sep="")
  linshi<-str_extract(qx_name1,"智慧")
  qx_name1<-gsub("智慧","",qx_name1)
  linshi[which(is.na(linshi))]<-""
  qx_name1[which(is.na(qx_name1))]<-""
  qx_name1<-paste(qx_name1,linshi,sep="")
  linshi<-str_extract(qx_name1,"经典")
  qx_name1<-gsub("经典","",qx_name1)
  linshi[which(is.na(linshi))]<-""
  qx_name1[which(is.na(qx_name1))]<-""
  qx_name1<-paste(qx_name1,linshi,sep="")
  linshi<-str_extract(qx_name1,"阳光")
  qx_name1<-gsub("阳光","",qx_name1)
  linshi[which(is.na(linshi))]<-""
  qx_name1[which(is.na(qx_name1))]<-""
  qx_name1<-paste(qx_name1,linshi,sep="")
  linshi<-str_extract(qx_name1,"领航")
  qx_name1<-gsub("领航","",qx_name1)
  linshi[which(is.na(linshi))]<-""
  qx_name1[which(is.na(qx_name1))]<-""
  qx_name1<-paste(qx_name1,linshi,sep="")
  linshi<-str_extract(qx_name1,"精锐|精英")
  qx_name1<-gsub("精锐|精英","",qx_name1)
  linshi[which(is.na(linshi))]<-""
  qx_name1[which(is.na(qx_name1))]<-""
  qx_name1<-paste(qx_name1,linshi,sep="")
  
  ##---对qx_name1清洗-------###
  qx_name1<-gsub("标准酷行","酷行",qx_name1)
  qx_name1<-gsub("豪华酷翔","酷翔",qx_name1)
  qx_name1<-gsub("舒适酷跃","酷跃",qx_name1)
  qx_name1<-gsub("新鸿达|鸿达|已备案|浩纳|索兰托|途安","",qx_name1)
  qx_name1<-gsub("精质","精致",qx_name1)
  qx_name1<-gsub("炫动套装","炫动佳",qx_name1)
  qx_name1<-gsub("智尚","致尚",qx_name1)
  qx_name1[grep("森雅S80",car_series1)]<-gsub("都市","",qx_name1[grep("森雅S80",car_series1)])
  qx_name1[grep("夏利",car_series1)]<-gsub("A佳|N3佳|A|N3","",qx_name1[grep("夏利",car_series1)])
  qx_name2<-gsub("PREMIUM","PRM",qx_name2)
  
  
  ########输出########
  output_data<-data.frame(car_name,car_year,car_restyle,car_series1,car_series3,benchi,car_drive,qx_name1,qx_name2,
                          car_door,car_liter_wu,car_desc,car_auto,car_site,car_oil,car_discharge_standard,car_hp,car_height_t,car_wheelbase,car_engine,car_engine1,car_OBD,car_discharge,car_price,car_model_name)
  return(output_data)
}