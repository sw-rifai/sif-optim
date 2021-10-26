# Description: Fit Topt of SIF by 3-week time windows in an attempt to 
# identify time required for acclimation.
# Author: Sami Rifai
# Date: 2021-10-19
# Notes: DO NOT RUN IN RSTUDIO. Not even with the jobs function.
#  Run on command line with following:
#  Rscript --no-save --no-restore --verbose src/R/fit_max-sif-3week-interval.R > outputFile.Rout 2>&1
# * memory hungry, needs 60+ gb
# * Still mysteriously fails sometimes, but this is likely caused by furrr
#   starting too many workers thank bankrupt the memory. 
#
# lc_class:
# 41: deciduous forest
# 42: evergreen forest 
# 43: mixed forest
# 

pacman::p_load(tidyverse,data.table,mgcv,nls.multstart,arrow,furrr,lubridate)
future::availableCores()
future::availableWorkers()
setwd("/home/sami/srifai@gmail.com/work/research/sif-optim/")
print(getwd())
source("src/R/functions_sif.R")
 
# options -----------------------------------------------------------------
lc_class <- 43
min_nobs <- 10
n_cores <- 20
n_splits <- 20
start_split_idx <- 1

# futures -----------------------------------------------------------------
options(mc.cores=n_cores)

# Data prep ---------------------------------------------------------------
if(file.exists(paste0("../data_general/proc_sif-optim/parquet-by-lc/sif_lst_pdsi_lc",lc_class,".parquet"))==F){
  dat <- lapply(
    list.files("../data_general/proc_sif-optim/merged_parquets/",pattern = paste0("lc-",lc_class),full.names = T),
    arrow::read_parquet)
  dat <- rbindlist(dat)
  dat[,lc := lc_class]
  dat[,`:=`(lst = lst*0.02 - 273.15, 
            sif = sif/1000)][,`:=`(xc=round(x*4)/4,
    yc=round(y*4)/4)][,`:=`(month=month(date),
      year=year(date))]
  g <- unique(dat[,.(xc,yc)])[,.(id=.GRP),by=.(xc,yc)]
  dat <- merge(dat,g,by=c("xc","yc"))
  dat[,`:=`(fid = factor(id))]
  dat[,`:=`(dc = case_when(pdsi <= -2 ~ "drought",
                           pdsi > -2 & pdsi < 2 ~ "norm",
                           pdsi >= 2 ~ "wet"))]
  
  
  nobs <- dat[,.(nobs=.N),by=.(id,dc)]
  dat <- merge(dat,nobs,by=c("id","dc"))
  dat <- dat[nobs >= min_nobs]
  
  arrow::write_parquet(dat,
    sink=paste0("../data_general/proc_sif-optim/parquet-by-lc/sif_lst_pdsi_lc",lc_class,".parquet"),compression = 'snappy')
}else{
  dat <- arrow::read_parquet(paste0("../data_general/proc_sif-optim/parquet-by-lc/sif_lst_pdsi_lc",lc_class,".parquet"))
}

dat[,`:=`(week=week(date))]
# Filter for max SIF ---------------------------------------------------
sdat <- dat[,`:=`(clst = round(lst,1))][,
  .(sif = max(sif), 
    pdsi = mean(pdsi,na.rm=T)),
  by=.(id,xc,yc,clst,year,month,week)]
setnames(sdat,'clst','lst')
sdat <- sdat[,`:=`(proc_id = .GRP), by=.(id,year,month,week)]

sdat0 <- copy(sdat)[,week:=week-1]
sdat1 <- copy(sdat)[,week:=week]
sdat2 <- copy(sdat)[,week:=week+1]

wdat <- rbindlist(list(sdat0,sdat1,sdat2))

snobs <- wdat[,.(nobs = .N),by=.(id,year,week)]
wdat <- merge(sdat,snobs,by=c('id','year','week'))
wdat <- wdat[nobs>=min_nobs]


# sdat[id==1][week==21] %>% ggplot(data=.,aes(lst,sif,color=month))+geom_point()
# wdat[id==1][week==21] %>% ggplot(data=.,aes(lst,sif,color=month))+geom_point()

# 
# d_weeks <- data.table(week_pair = paste(18:38,19:39), 
#            week_1 = 18:38, 
#            week_2 = 19:39)
# 
# sdat <- merge(sdat, d_weeks, by.x = 'week', by.y='week_1')
# sdat <- merge(sdat, d_weeks, by.x = 'week', by.y='week_2')

# sdat$week %>% unique %>% table
# wdat[id%in%sample(5067,20)][year==2018] %>%
#   # group_by(week_pair.x) %>% 
#   # mutate(med_lst = median(lst)) %>% 
#   ggplot(data=.,aes(lst,sif,color=factor(week)))+
#   geom_point(alpha=0.1)+
#   # geom_vline(aes(xintercept=med_lst,color=factor(week_pair.x)))+
#   geom_smooth(method='nls',
#     lwd=0.25,
#     formula=y~p_opt-b*(x-Topt)**2,
#     se=F,
#     method.args=list(
#       start=c(p_opt=10,b=1,Topt=25)
#       # algorithm='port'
#     ))+
#   scale_color_viridis_d()+
#   facet_wrap(~id)


# furrr parallel proc -----------------------------------------------
vec_ids <- unique(wdat$proc_id)
l_ids <- split(vec_ids, cut(seq_along(vec_ids), n_splits, labels = FALSE))

plan(cluster, workers=n_cores)
all_out <- list()
for(i in start_split_idx:n_splits){
  print(paste("starting split ",i));
  gc(full=T)
  system.time(all_out[[i]] <- wdat[proc_id%in%l_ids[[i]]] %>%
                split(f=list(.$id,.$year,.$week),
                  drop=T) %>%
                future_map(~(paf_dt_3week(.x,iter = 10,force_pa = T,min_nobs = min_nobs)),
                  .progress = F,
                  .options = furrr_options(seed=333L)) %>%
    rbindlist()
)

    print(paste("writing split ",i));
  arrow::write_parquet(all_out[[i]], sink=paste0("../data_general/proc_sif-optim/mod_fits_max_sif/",
  "lc-",lc_class,"-max-sif-fits_split-",i,"_",Sys.Date(),".parquet"),
  compression = 'snappy')
  
}

gc(full=T)
all_out <- rbindlist(all_out)
gc(full=T)
arrow::write_parquet(all_out, sink=paste0("../data_general/proc_sif-optim/mod_fits_max_sif_monthly/",
  "lc-",lc_class,"-max-sif-3week-interval_fits_",Sys.Date(),".parquet"),
  compression = 'snappy')


# # scratch --- delete ---------------------------------
# all_out[term=='topt']
# 
# 
# paf_dt_monthly(wdat[proc_id%in%l_ids[[i]]] %>%
#                 split(f=list(.$id,.$year,.$week),
#                   drop=T) %>% .[[3]], force_pa = T)
# 
# 
# test <- wdat[proc_id%in%l_ids[[i]]] %>%
#                 split(f=list(.$id,.$year,.$week),
#                   drop=T) %>%
#                 future_map(~(paf_dt_3week(.x,iter = 20,force_pa = T,min_nobs = min_nobs)),
#                   .progress = F,
#                   .options = furrr_options(seed=333L)) %>%
#     rbindlist()
# 
# test[term=='Topt'][p.value<0.05]
# 
# all_out[term=='Topt'][p.value<0.05][std.error < 1.5] %>% 
#   ggplot(data=.,aes(week_center,estimate,color=factor(id)))+
#   geom_point()
