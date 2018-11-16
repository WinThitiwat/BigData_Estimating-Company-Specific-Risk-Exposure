
#mymodel <- glm(rating ~ at + cogs + dltt + dp + capx, data=traingood)


library(readr)
library(dplyr)


FUND_VARIABLES <- c("act","chech","cogs","cshi","csho","dclo","dltt","dp","dvt","ebit",
                         "epsfi","epsfx","fincf","gdwl","ib","invt","ivncf","lt","ni","optvol",
                         "re","rea","tstk")

#--SCA_VARIABLES <- c("Ticker","SettlementAmount","FilingYear")
SCA_VARIABLES <- c("tic","SettlementAmount", "FilingYear")

SECURE_VARIABLES <- c("tic","prccm")


fundamental_df <- read_csv("D:/E-Book/Hult/Module E/Big Data Analytics/Project/Fundamentals_2.csv")

sca_df <- read_csv("D:/E-Book/Hult/Module E/Big Data Analytics/Project/SCA.csv")

securities_df <- read_csv("D:/E-Book/Hult/Module E/Big Data Analytics/Project/securities.csv")

length(intersect(fundamental_df$tic, sca_df$tic))

selected_fundamental_df <- fundamental_df[FUND_VARIABLES]

#fundamental_df[FUND_VARIABLES] <- no_nas_fund_df

selected_sca_df <- sca_df[SCA_VARIABLES]

selected_securities_df <- securities_df[SECURE_VARIABLES]

# null_idx <- which(selected_sca_df$SettlementAmount=="#NULL!")
# selected_sca_df$SettlementAmount[null_idx] <- "0"



#no_nas_fund_df <- replaceMedianInNa(selected_fundamental_df, FUND_VARIABLES)

#no_nas_securities_df <- replaceMedianInNa(selected_securities_df, SECURE_VARIABLES)


# --------> Fill NA data in the original data -------
#fundamental_df[FUND_VARIABLES] <- no_nas_fund_df

#securities_df[SECURE_VARIABLES] <- no_nas_securities_df


#--------> Group the Fundamental file  -----------


consolidated_fund_df <- fundamental_df %>%
    select(tic,act,"chech","cogs","cshi","csho","dclo","dltt","dp","dvt","ebit",
           "epsfi","epsfx","fincf","gdwl","ib","invt","ivncf","lt","ni","optvol",
           "re","rea","tstk" ) %>%
    group_by(tic) %>%
    summarise(act = ifelse(all(is.na(act)),0,median(act, na.rm = TRUE)), 
              chech = ifelse(all(is.na(chech)),0,median(chech, na.rm = TRUE)),
              cogs = ifelse(all(is.na(cogs)),0,median(cogs, na.rm = TRUE)), 
              cshi = ifelse(all(is.na(cshi)),0,median(cshi, na.rm = TRUE)), 
              csho = ifelse(all(is.na(csho)),0,median(csho, na.rm = TRUE)), 
              dclo = ifelse(all(is.na(dclo)),0,median(dclo, na.rm = TRUE)), 
              dltt = ifelse(all(is.na(dltt)),0,median(dltt, na.rm = TRUE)), 
              dp = ifelse(all(is.na(dp)),0,median(dp, na.rm = TRUE)), 
              dvt = ifelse(all(is.na(dvt)),0,median(dvt, na.rm = TRUE)),  
              ebit = ifelse(all(is.na(ebit)),0,median(ebit, na.rm = TRUE)), 
              epsfi = ifelse(all(is.na(epsfi)),0,median(epsfi, na.rm = TRUE)), 
              epsfx = ifelse(all(is.na(epsfx)),0,median(epsfx, na.rm = TRUE)),  
              fincf = ifelse(all(is.na(fincf)),0,median(fincf, na.rm = TRUE)), 
              gdwl = ifelse(all(is.na(gdwl)),0,median(gdwl, na.rm = TRUE)), 
              ib = ifelse(all(is.na(ib)),0,median(ib, na.rm = TRUE)), 
              invt = ifelse(all(is.na(invt)),0,median(invt, na.rm = TRUE)), 
              ivncf = ifelse(all(is.na(ivncf)),0,median(ivncf, na.rm = TRUE)), 
              lt = ifelse(all(is.na(lt)),0,median(lt, na.rm = TRUE)),  
              ni = ifelse(all(is.na(ni)),0,median(ni, na.rm = TRUE)), 
              optvol = ifelse(all(is.na(optvol)),0,median(optvol, na.rm = TRUE)), 
              re = ifelse(all(is.na(re)),0,median(re, na.rm = TRUE)),  
              rea = ifelse(all(is.na(rea)),0,median(rea, na.rm = TRUE)),  
              tstk = ifelse(all(is.na(tstk)),0,median(tstk, na.rm = TRUE)))
    


consolidated_securities_df <- securities_df %>%
    select(tic, "prccm") %>%
    group_by(tic) %>%
    summarise(prccm = ifelse(all(is.na(prccm)),0,median(prccm, na.rm = TRUE)))


settlement_cleansing <- function(duplicated_df){
    new_sca_df <- data.frame(tic=character(), SettlementAmount = double())
    result <- 0
    atTicker <- 1
    atSettlementAmount <- 2
    atFilingYear <- 3
    
    # check if there is numeric values exists in the duplicated dataset
    for (cur_ticker in unique(duplicated_df$tic)){
        theseTickerRow <- duplicated_df[which(duplicated_df$tic==cur_ticker),]
        
        # check if there is only single entry of this ticker
        # if(nrow(theseTickerRow)==1){  #!any(duplicated(theseTickerRow$tic))
        #     new_sca_df <- rbind(new_sca_df, 
        #                              data.frame(tic=unique(theseTickerRow$tic), SettlementAmount = 0))
        #     #ifelse(identical(theseTickerRow$SettlementAmount,"#NULL!"),0,theseTickerRow$SettlementAmount)
        # }
        # in case there are multiple entries of this ticker
        # else{
            if(sum(theseTickerRow$SettlementAmount!="#NULL!")==1){
                print("1")
                new_sca_df <- rbind(new_sca_df, data.frame(tic=unique(theseTickerRow$tic), SettlementAmount = theseTickerRow$SettlementAmount[theseTickerRow$SettlementAmount!="#NULL!"]))
            }
            else if (sum(theseTickerRow$SettlementAmount!="#NULL!") >=2 ) {
                max_val <- unique(theseTickerRow$SettlementAmount[which.max(theseTickerRow$FilingYear[which(theseTickerRow$SettlementAmount!="#NULL!")])])
                
                new_sca_df <- rbind(new_sca_df, data.frame(tic=unique(theseTickerRow$tic), SettlementAmount = max_val))
            }
            else{
                print("else")
                new_sca_df <- rbind(new_sca_df, data.frame(tic=unique(theseTickerRow$tic), SettlementAmount =0))
            }
        # }
    }
    return(new_sca_df)
}

new_sca_df <- settlement_cleansing(sca_df)

# consolidated_sca_df  <- sca_df %>%
#     select(tic,"SettlementAmount", "FilingYear") %>%
#     group_by(tic) %>%
#     summarise(SettlementAmount = settlement_cleansing(sca_df))


    
#----------> Merging datasets


merge_fund_secure_df <- merge(consolidated_fund_df, consolidated_securities_df, by = "tic")
 
all_merge_df <- merge(merge_fund_secure_df, new_sca_df, by="tic")


write.csv(all_merge_df, file = "C:/Users/Admin/Desktop/all_merge_df.csv")

x <- all_merge_df

x$All_merge_df <- as.numeric(x$All_merge_df)

fit <- lm(suppressWarnings(as.numeric(SettlementAmount))~act+chech+cogs+ cshi+ csho+dclo+ dltt+dp+ dvt+ebit+epsfi+ epsfx+fincf+gdwl+ib+invt+ivncf+lt+ni+ optvol+re+rea+tstk+prccm, data = all_merge_df)
glmFit <- glm(suppressWarnings(as.numeric(SettlementAmount))~act+chech+cogs+ cshi+ csho+dclo+ dltt+dp+ dvt+ebit+epsfi+ epsfx+fincf+gdwl+ib+invt+ivncf+lt+ni+ optvol+re+rea+tstk+prccm, data=all_merge_df,family = 'binomial')
summary(glmFit)
summary(fit)

index <- sample(1:nrow(all_merge_df),round(0.75*nrow(all_merge_df)))
train <- all_merge_df[index,]
test <- all_merge_df[-index,]
lm.fit <- glm(as.numeric(SettlementAmount)~., data=train)
summary(lm.fit)
pr.lm <- predict(lm.fit,test)