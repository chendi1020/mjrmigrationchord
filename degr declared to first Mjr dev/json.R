source("H:/R setup/ODBC Connection.R")
source("H:/R setup/OracleDbusing ROracle.R")
#for short name of college
COLLNM <- sqlFetch(SISInfo, 'COLLEGE')

#get the paramter
par <- dbGetQuery(MSUEDW, "select *
                from OPB_PERS_FALL.STANDARD_REPORT_PARAMETERS_V ")
maxyr <-par[par$PARAMETER_NAME=='PAG_GRADUATING_COHORT_CEILING_YEAR','PARAMETER_VALUE_TEXT']
#most recent grad cohorts
gradcohort <- dbGetQuery(MSUEDW, paste0( "select distinct GRADUATING_COHORT, count(distinct PID) as count
                                  from  OPB_PERS_FALL.PERSISTENCE_V
                                  where GRADUATING_COHORT is not null and
                                      student_level='UN' and level_entry_status='FRST' and 
                                 (ENTRANT_SUMMER_FALL='Y' or substr(ENTRY_TERM_CODE,1,1)='F')
                                  and GRADUATING_COHORT <='",maxyr,
                                  "' group by GRADUATING_COHORT
                                   order by GRADUATING_COHORT desc
                         "))

cohort <- gradcohort$GRADUATING_COHORT[1:5]

PAG <- data.frame()
for (i in cohort){
        PAGds <- dbGetQuery(MSUEDW, paste0("select distinct Pid, GRADUATING_COHORT, COLLEGE_FIRST,  COLLEGE_DEGREE, DEPT_FIRST, DEPT_FIRST_NAME, DEPT_DEGREE,
                DEPT_DEGREE_NAME,
                MAJOR_FIRST_SEMESTER, MAJOR_NAME_FIRST, MAJOR_DEGREE, MAJOR_NAME_DEGREE
                                 from OPB_PERS_FALL.PERSISTENCE_V
                                 where student_level='UN' and level_entry_status='FRST' and (ENTRANT_SUMMER_FALL='Y' or substr(ENTRY_TERM_CODE,1,1)='F')
                                 and  GRADUATING_COHORT = '", i,"'" ,sep="") ) 
        PAG <- rbind(PAG, PAGds)
}


#use the short college name
library(sqldf)
PAG <- sqldf("select a.*, b.Short_Name as COLLEGE_FIRST_NAME, c.Short_Name as COLLEGE_DEGREE_NAME
             from PAG a 
             left join COLLNM b 
             on a.COLLEGE_FIRST=b.Coll_Code
             left join COLLNM c 
             on a.COLLEGE_DEGREE=c.Coll_Code")


setwd("H:/GitHub/degr declared to first Mjr dev/json")
#build loop around the degree college
degrcoll <- unique(PAG$COLLEGE_DEGREE)

#loop through each degree college
for (k in degrcoll){
        data <- PAG[PAG$COLLEGE_DEGREE==k,]
        
        #concate to prevent same character
        data$COLLEGE_FIRST_NAME <- paste(data$COLLEGE_FIRST, data$COLLEGE_FIRST_NAME, sep = "-")
        data$MAJOR_NAME_FIRST <- paste(data$MAJOR_FIRST_SEMESTER, data$MAJOR_NAME_FIRST, sep = "-")
        
        data$COLLEGE_DEGREE_NAME <- paste(data$COLLEGE_DEGREE, data$COLLEGE_DEGREE_NAME, sep = "-")
        data$MAJOR_NAME_DEGREE <- paste(data$MAJOR_DEGREE, data$MAJOR_NAME_DEGREE, sep = "-")
        
        #aggregation
        library(dplyr)
        
        Agg1 <- data %>% group_by(GRADUATING_COHORT, COLLEGE_FIRST_NAME,COLLEGE_DEGREE_NAME ) %>% summarise(count=n())
        Agg2 <- data %>% group_by(GRADUATING_COHORT, COLLEGE_FIRST_NAME,COLLEGE_DEGREE_NAME, MAJOR_NAME_FIRST,  MAJOR_NAME_DEGREE) %>% summarise(count=n())
        Agg3 <- data %>% group_by(GRADUATING_COHORT,  MAJOR_NAME_FIRST,  MAJOR_NAME_DEGREE) %>% summarise(count=n())
        Agg4 <- data %>% group_by(GRADUATING_COHORT, COLLEGE_FIRST_NAME,  MAJOR_NAME_DEGREE) %>% summarise(count=n())
        Agg5 <- data %>% group_by(GRADUATING_COHORT, MAJOR_NAME_FIRST,  COLLEGE_DEGREE_NAME) %>% summarise(count=n())
        
        #gather all college and major from 2 time points
        
        DS1<- sqldf("select distinct   COLLEGE_DEGREE_NAME as coll,  MAJOR_NAME_DEGREE as mjr
                    from Agg2
                    union
                    select distinct COLLEGE_FIRST_NAME , MAJOR_NAME_FIRST
                    from Agg2")
        
        #for each college, build college following by majors within that college list
        t<- split(DS1, DS1$coll)
        listf <- function(x){ c(unique(x$coll),x$mjr)}
        test <- sapply(t, listf)
        listall <- as.data.frame(do.call(c,test))
       
        
        colnames(listall) <- "Org"
        listall$merge <-1
        
        #main structure  for building the square matrix
        DS2 <- sqldf("select a.org, b.org as org1
                     from listall a, listall b
                     on 1=1
                     ")
        
        
        lvl <- unique(DS2$org1)
        
        levels(DS2$Org) <- lvl
        #covert org1 from char to factor
        DS2$org1 <- as.factor(DS2$org1)
        levels(DS2$org1) <- lvl
        
        
        
        names(Agg3)<-names(Agg1)
        names(Agg4) <- names(Agg1)
        names(Agg5) <- names(Agg1)
        mainds <- rbind(Agg1, Agg3, Agg4, Agg5)
        
        
        cohortseq <- unique(mainds$GRADUATING_COHORT)
        
        
        #for all choices, all five graduating cohort together
        DS2_2011_15 <- sqldf("select  a.org, a.org1, (case when b.count is null then 0 else b.count end) as count
                             from DS2 a
                             left join mainds b
                             on a.org=COLLEGE_DEGREE_NAME and a.org1=COLLEGE_FIRST_NAME  ")
        xtb_2011_15 <- xtabs(count ~ Org + org1,data=DS2_2011_15)
        
        
        
        #all five graduating cohorts from 2011-12 to 2015-16
        
        vec_2011_15<-vector()
        for(i in  seq(nrow(xtb_2011_15))) {
                x<-c(xtb_2011_15[nrow(xtb_2011_15)+1-i,])
                names(x)<-NULL
                vec_2011_15<-rbind(x,vec_2011_15)
                
        }
        
        
        #loop through each graduating cohort
        #vec <- list("All"=vec_2011_15)
        vec <- list()
        
        for (j in cohortseq){
                mds_2009 <- mainds%>% filter(GRADUATING_COHORT==j)
                
                DS2_2009 <- sqldf("select  a.org, a.org1, (case when b.count is null then 0 else b.count end) as count
                                  from DS2 a
                                  left join mds_2009 b
                                  on  a.org=COLLEGE_DEGREE_NAME and a.org1=COLLEGE_FIRST_NAME  ")
                
                xtb_2009 <- xtabs(count ~ Org + org1,data=DS2_2009)
                vec_2009<-vector()
                for(i in  seq(nrow(xtb_2009))) {
                        x<-c(xtb_2009[nrow(xtb_2009)+1-i,])
                        names(x)<-NULL
                        vec_2009<-rbind(x,vec_2009)
                        
                        curlist <- list(vec_2009)
                        names(curlist) <- paste(substr(j,1,4), substr(j,8,9), sep = "-")
                }
               
                vec <- c(vec,curlist)
                
        }
        
        vec <- c(vec, list("All"=vec_2011_15))
        
        #region
        coll <- unique(DS1$coll)
        
        
        regionnum <- sapply(coll, function(x){ which(lvl== x)-1})
        names(regionnum) <- NULL
        
        
        names <- lvl
        
        
        
        list <- list("names"=names, "regions"=regionnum, "matrix"=vec)
        
        require(RJSONIO)
       
        
        jsonOut<-toJSON(list)
        #cat(jsonOut)
        
        sink(paste('data',k, '.json', collapse ='', sep=""))
        cat(jsonOut)
        
        sink()
        
        
       
        
}
        
        
        

       
        
        
        
###########csv###############
for (y in degrcoll){
        
        data <- PAG[PAG$COLLEGE_DEGREE==y,]
        
        #recode those who have not graduated yet
        #data$COLLEGE_DEGREE <- ifelse(is.na(data$COLLEGE_DEGREE),99, data$COLLEGE_DEGREE)
        data$COLLEGE_DEGREE_NAME <- as.character(data$COLLEGE_DEGREE_NAME)
        #data$COLLEGE_DEGREE_NAME <- ifelse(is.na(data$COLLEGE_DEGREE_NAME),'Not Graduate', data$COLLEGE_DEGREE_NAME)
        
        #data$DEPT_DEGREE<- ifelse(is.na(data$DEPT_DEGREE),999, data$DEPT_DEGREE)
        data$DEPT_DEGREE_NAME <- as.character(data$DEPT_DEGREE_NAME)
        #data$DEPT_DEGREE_NAME <- ifelse(is.na(data$DEPT_DEGREE_NAME),'Not Graduate', data$DEPT_DEGREE_NAME)
        
        #data$MAJOR_DEGREE <- ifelse(is.na(data$MAJOR_DEGREE), 9999, data$MAJOR_DEGREE)
        data$MAJOR_NAME_DEGREE <- as.character(data$MAJOR_NAME_DEGREE)
        #data$MAJOR_NAME_DEGREE <- ifelse(is.na(data$MAJOR_NAME_DEGREE), 'Not Graduate', data$MAJOR_NAME_DEGREE)
        
        #concate to prevent same character
        data$COLLEGE_FIRST_NAME <- paste(data$COLLEGE_FIRST, data$COLLEGE_FIRST_NAME, sep = "-")
        data$DEPT_FIRST_NAME <- paste(data$DEPT_FIRST, data$DEPT_FIRST_NAME, sep="-")
        data$MAJOR_NAME_FIRST <- paste(data$MAJOR_FIRST_SEMESTER, data$MAJOR_NAME_FIRST, sep = "-")
        
        data$COLLEGE_DEGREE_NAME <- paste(data$COLLEGE_DEGREE, data$COLLEGE_DEGREE_NAME, sep = "-")
        data$DEPT_DEGREE_NAME <- paste(data$DEPT_DEGREE, data$DEPT_DEGREE_NAME, sep="-")
        data$MAJOR_NAME_DEGREE <- paste(data$MAJOR_DEGREE, data$MAJOR_NAME_DEGREE, sep = "-")
        
        
        
        #aggregation
        library(dplyr)
        library(xlsx)
        
        #first college
        cdat <- data.frame()
        for (j in c('COLLEGE_DEGREE_NAME','DEPT_DEGREE_NAME','MAJOR_NAME_DEGREE')){
                for (k in c('COLLEGE_FIRST_NAME','DEPT_FIRST_NAME','MAJOR_NAME_FIRST')){
                        AG1 <- data %>% group_by_('GRADUATING_COHORT',j,k)%>% summarise(count=n())
                        AG1$firsttype <- substr( k,1,regexpr('_',k)-1)
                        AG1$degtype <-  substr( j,1,regexpr('_',j)-1)
                        names(AG1) <- c('GRADUATING_COHORT','First','Degree','Headcount','firsttype','degtype')
                        AG1 <- AG1[,c('GRADUATING_COHORT', 'degtype','Degree','firsttype' ,'First','Headcount')]
                        cdat <- rbind(cdat,as.data.frame(AG1))
                }
        }
        
        names(cdat)<- c('GRADUATING_COHORT','Degree_Unit_Type','Degree_College_Dept_Majr', 'First_Unit_Type','First_College_Dept_Majr','Headcount')
        
        
        
        ##Agg1 <- data %>% group_by(COHORT, COLLEGE_FIRST_NAME,COLLEGE_DEGREE_NAME ) %>% summarise(count=n())
        #Agg2 <- data %>% group_by(COHORT, COLLEGE_FIRST_NAME,MAJOR_NAME_FIRST, COLLEGE_DEGREE_NAME,  MAJOR_NAME_DEGREE) %>% summarise(count=n())
        #Agg2all <- data %>% mutate(MAJOR_NAME_DEGREE= paste0(COLLEGE_DEGREE_NAME,'-All')) %>%
        #       group_by(COHORT, COLLEGE_FIRST_NAME,MAJOR_NAME_FIRST, COLLEGE_DEGREE_NAME,  MAJOR_NAME_DEGREE) %>% summarise(count=n())
        #Agg3 <- rbind(as.data.frame(Agg2all), as.data.frame(Agg2)) %>%
        #       arrange(COHORT, COLLEGE_FIRST_NAME,MAJOR_NAME_FIRST, COLLEGE_DEGREE_NAME,  MAJOR_NAME_DEGREE)%>% select(-c(COLLEGE_DEGREE_NAME))
        
        #Agg3all <- Agg3 %>% mutate(MAJOR_NAME_FIRST= paste0(COLLEGE_FIRST_NAME,'-All')) %>% 
        #       group_by(COHORT, COLLEGE_FIRST_NAME,MAJOR_NAME_FIRST,  MAJOR_NAME_DEGREE) %>% summarise(count=sum(count))
        
        #Agg1 <- rbind(as.data.frame(Agg3all), as.data.frame(Agg3)) %>%
        #       arrange(COHORT, COLLEGE_FIRST_NAME,MAJOR_NAME_FIRST,   MAJOR_NAME_DEGREE)%>% select(-c(COLLEGE_FIRST_NAME))
        
        #names(Agg1) <- c('Entering_Cohort','First_College_Major','Degree_College_Major','Headcount')
        
        #Agg1all <- Agg1  %>% group_by( First_College_Major, Degree_College_Major) %>% 
        #       summarise(Headcount=sum(Headcount)) %>% mutate(Entering_Cohort='All') %>% select(Entering_Cohort, First_College_Major, Degree_College_Major, Headcount)
        
        #Agg1 <- rbind(as.data.frame(Agg1all), Agg1) %>% arrange(Entering_Cohort, First_College_Major, Degree_College_Major)
        
        for (n in unique(cdat$GRADUATING_COHORT)){
                write.xlsx2(cdat[cdat$GRADUATING_COHORT==n,],
                            file = paste0("H:/GitHub/degr declared to first Mjr dev/json/csv/data",y,".xlsx"), sheetName = paste0('Graduat_Cohort_',n), append = T,
                            showNA="", row.names = F)
        }
        
}

        
       
        
      
        
      
    
        
       
      
        