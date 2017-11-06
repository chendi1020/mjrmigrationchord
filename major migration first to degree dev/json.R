source("S:/Institutional Research/Chen/R setup/ODBC Connection.R")
library(sqldf)
PAGmajrH <- sqlQuery(MSUEDW, "select distinct cohort,Pid,
                     MAJOR_US1, 
                     MAJOR_FS1, 
                     MAJOR_SS1, 
                     MAJOR_US2, 
                     MAJOR_FS2,
                     MAJOR_SS2,
                     MAJOR_US3, 
                     MAJOR_FS3, 
                     MAJOR_SS3, 
                     MAJOR_US4,
                     MAJOR_FS4,
                     MAJOR_SS4, 
                     MAJOR_US5, 
                     MAJOR_FS5,
                     MAJOR_SS5,
                     MAJOR_US6, 
                     MAJOR_FS6, 
                     MAJOR_SS6, 
                     MAJOR_US7, 
                     MAJOR_FS7, 
                     MAJOR_SS7, 
                     MAJOR_US8, 
                     MAJOR_FS8, 
                     MAJOR_SS8,
                     MAJOR_US9, 
                     MAJOR_FS9, 
                     MAJOR_SS9, 
                     MAJOR_US10, 
                     MAJOR_FS10, 
                     MAJOR_SS10,
                     MAJOR_DEGREE,
                     MAJOR_NAME_FIRST
                     from OPB_PERS_FALL.PERSISTENCE_V 
                     where STUDENT_LEVEL='UN' and LEVEL_ENTRY_STATUS='FRST' and COHORT in (2009,2008,2007,2006,2005)
                     and MAJOR_NAME_FIRST='No Preference'
                     ")

library(reshape)
PAGmajrV <- melt(PAGmajrH, id=c('COHORT','PID','MAJOR_DEGREE','MAJOR_NAME_FIRST'))
PAGmajrV1 <- PAGmajrV[! is.na( PAGmajrV$value),]
PAGmajrV1$pick <- ifelse(PAGmajrV1$value=='5151',0,1)

library(plyr)
majr_1st<-ddply(.data=PAGmajrV1, .var=c("COHORT","PID","pick"), .fun=function(x)x[1,])

library(dplyr)
mjr <- majr_1st %>% group_by(COHORT, PID) %>% summarise(p=sum(pick))
mjr1 <- sqldf("select b.*
              from mjr a 
              inner join majr_1st b 
              on a.Pid=b.Pid and a.COHORT=b.COHORT and a.p=b.pick")


majrmnt <- sqlFetch(SISInfo, 'MAJORMNT')
College <- sqlFetch(SISInfo, 'COLLEGE')
majrmnt$majrcode <- ifelse( is.na(as.numeric( as.character(majrmnt$Major_Code) )), as.character(majrmnt$Major_Code),
                            as.numeric( as.character(majrmnt$Major_Code) ))

majr <- sqldf("select a.*, b.Short_Desc as mjr_sn_1st, b.Long_Desc as mjr_ln_1st, 
              c.Short_Name as coll_sn_1st, 
              c.Full_Name as coll_fn_1st, 
              c.coll_code as coll_code_1st,
              b1.Long_Desc as mjr_ln_dgr, 
              b1.Coll_code as coll_code_dgr,
              c1.Full_Name as coll_fn_dgr
             
              from mjr1 a  
              left join majrmnt  b  
              on a.value=b.majrcode
              left join College c  
              on b.coll_code=c.coll_code
              left join majrmnt  b1  
              on a.MAJOR_DEGREE=b1.majrcode
              left join College c1 
              on b1.coll_code=c1.coll_code
              ")

majr$Coll1st <- ifelse(majr$coll_code_1st==43, 'Never Declare Majr-99',   paste0( majr$coll_fn_1st,'-',majr$coll_code_1st) )
majr$majr1st <- ifelse(majr$value==5151, 'Never Declare Majr-9999',  paste0( majr$mjr_ln_1st,'-',majr$value) )
majr$Colldgr <- ifelse(is.na(majr$coll_code_dgr), 'Not Graduate-88', paste0(majr$coll_fn_dgr,'-',majr$coll_code_dgr))
majr$majrdgr <- ifelse(is.na(majr$MAJOR_DEGREE), 'Not Graduate-8888', paste0(majr$mjr_ln_dgr,'-',majr$MAJOR_DEGREE))




library(dplyr)

Agg1 <- majr %>% group_by(COHORT, Coll1st,Colldgr ) %>% summarise(count=n())
Agg2 <- majr %>% group_by(COHORT, Coll1st,Colldgr, majr1st,  majrdgr) %>% summarise(count=n())
Agg3 <- majr %>% group_by(COHORT,  majr1st,  majrdgr) %>% summarise(count=n())
Agg4 <- majr %>% group_by(COHORT, Coll1st, majrdgr) %>% summarise(count=n())
Agg5 <- majr %>% group_by(COHORT, majr1st,  Colldgr) %>% summarise(count=n())

DS1<- sqldf("select distinct Coll1st as coll, Majr1st as mjr
            from Agg2
            union
            select distinct Colldgr ,majrdgr
            from Agg2")


t<- split(DS1, DS1$coll)
listf <- function(x){ c(unique(x$coll),x$mjr)}
test <- sapply(t, listf)
listall <- as.data.frame(do.call(c,test))
#listall<-as.data.frame(listall[!duplicated(listall), ])
colnames(listall) <- "Org"
listall$merge <-1


DS2 <- sqldf("select a.org, b.org as org1
             from listall a, listall b
             on 1=1
             ")



lvl <- unique(DS2$org1)


levels(DS2$Org) <- lvl
DS2$org1 <- as.factor(DS2$org1)
levels(DS2$org1) <- lvl



names(Agg3)<-names(Agg1)
names(Agg4) <- names(Agg1)
names(Agg5) <- names(Agg1)
mainds <- rbind(Agg1, Agg3, Agg4, Agg5)


#2009
mds_2009 <- mainds%>% filter(COHORT==2009)

DS2_2009 <- sqldf("select  a.org, a.org1, (case when b.count is null then 0 else b.count end) as count
                  from DS2 a
                  left join mds_2009 b
                  on a.org=Coll1st and a.org1=Colldgr ")
xtb_2009 <- xtabs(count ~ Org + org1,data=DS2_2009)

#2008
mds_2008 <- mainds%>% filter(COHORT==2008)

DS2_2008 <- sqldf("select  a.org, a.org1, (case when b.count is null then 0 else b.count end) as count
                  from DS2 a
                  left join mds_2008 b
                  on a.org=Coll1st and a.org1=Colldgr ")
xtb_2008 <- xtabs(count ~ Org + org1,data=DS2_2008)


#2007
mds_2007 <- mainds%>% filter(COHORT==2007)

DS2_2007 <- sqldf("select  a.org, a.org1, (case when b.count is null then 0 else b.count end) as count
                  from DS2 a
                  left join mds_2007 b
                  on a.org=Coll1st and a.org1=Colldgr ")
xtb_2007 <- xtabs(count ~ Org + org1,data=DS2_2007)

#2006
mds_2006 <- mainds%>% filter(COHORT==2006)

DS2_2006 <- sqldf("select  a.org, a.org1, (case when b.count is null then 0 else b.count end) as count
                  from DS2 a
                  left join mds_2006 b
                  on a.org=Coll1st and a.org1=Colldgr ")
xtb_2006 <- xtabs(count ~ Org + org1,data=DS2_2006)

#2005
mds_2005 <- mainds%>% filter(COHORT==2005)

DS2_2005 <- sqldf("select  a.org, a.org1, (case when b.count is null then 0 else b.count end) as count
                  from DS2 a
                  left join mds_2005 b
                  on a.org=Coll1st and a.org1=Colldgr ")
xtb_2005 <- xtabs(count ~ Org + org1,data=DS2_2005)

#2005-2009
DS2_2005_09 <- sqldf("select  a.org, a.org1, (case when b.count is null then 0 else b.count end) as count
                     from DS2 a
                     left join mainds b
                     on a.org=Coll1st and a.org1=Colldgr ")
xtb_2005_09 <- xtabs(count ~ Org + org1,data=DS2_2005_09)







#2009 new

vec_2009<-vector()
for(i in  seq(nrow(xtb_2009))) {
  x<-c(xtb_2009[nrow(xtb_2009)+1-i,])
  names(x)<-NULL
  vec_2009<-rbind(x,vec_2009)
  
}


#2008 new

vec_2008<-vector()
for(i in  seq(nrow(xtb_2008))) {
  x<-c(xtb_2008[nrow(xtb_2008)+1-i,])
  names(x)<-NULL
  vec_2008<-rbind(x,vec_2008)
  
}


#2007 new

vec_2007<-vector()
for(i in  seq(nrow(xtb_2007))) {
  x<-c(xtb_2007[nrow(xtb_2007)+1-i,])
  names(x)<-NULL
  vec_2007<-rbind(x,vec_2007)
  
}

#2006 new

vec_2006<-vector()
for(i in  seq(nrow(xtb_2006))) {
  x<-c(xtb_2006[nrow(xtb_2006)+1-i,])
  names(x)<-NULL
  vec_2006<-rbind(x,vec_2006)
  
}



#2005 new

vec_2005<-vector()
for(i in  seq(nrow(xtb_2005))) {
  x<-c(xtb_2005[nrow(xtb_2005)+1-i,])
  names(x)<-NULL
  vec_2005<-rbind(x,vec_2005)
  
}

#2005 to 2009 new

vec_2005_09<-vector()
for(i in  seq(nrow(xtb_2005_09))) {
  x<-c(xtb_2005_09[nrow(xtb_2005_09)+1-i,])
  names(x)<-NULL
  vec_2005_09<-rbind(x,vec_2005_09)
  
}




#region
coll <- unique(DS1$coll)


regionnum <- sapply(coll, function(x){ which(lvl== x)-1})
names(regionnum) <- NULL

pos <- function(x) {
  rev(gregexpr("-",x)[[1]])[1]
  
}
position <- sapply(lvl, pos)

names <-substr(lvl,1,position-1) 


listtest <- list("2009"=vec_2009, "2008"=vec_2008,"2007"=vec_2007, "2006"=vec_2006,"2005"=vec_2005, "All"=vec_2005_09)
list <- list("names"=names, "regions"=regionnum, "matrix"=listtest)

require(RJSONIO)
#test<-paste0(row1x,",",row2x,collapse = "")
#names(test)<-"test1"

jsonOut<-toJSON(list)
#cat(jsonOut)

sink('data.json')
cat(jsonOut)

sink()

