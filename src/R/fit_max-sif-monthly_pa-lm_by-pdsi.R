# Description: Fit Topt or linear mods for max SIF per 0.1 degree LST
# Author: Sami Rifai
# Date: 2021-09-07
# Notes: DO NOT RUN IN RSTUDIO. Not even with the jobs function.
#  Run on command line with following:
#  Rscript --no-save --no-restore --verbose src/R/fit_max-sif-monthly_pa-lm_by-pdsi.R > outputFile.Rout 2>&1
# * memory hungry, needs 60+ gb
# * Still mysteriously fails sometimes, but this is likely caused by furrr
#   starting too many workers thank bankrupt the memory. 
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
min_nobs <- 100
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

# Filter for max SIF ---------------------------------------------------
sdat <- dat[,`:=`(clst = round(lst,1))][
  ,.(sif = max(sif), 
    pdsi = mean(pdsi,na.rm=T)),
  by=.(id,xc,yc,clst,year,month)]
setnames(sdat,'clst','lst')
snobs <- sdat[,.(nobs = .N),by=.(id,year,month)]
sdat <- merge(sdat,snobs,by=c('id','year','month'))
sdat <- sdat[nobs>=min_nobs]
sdat <- sdat[,`:=`(proc_id = .GRP), by=.(id,year,month)]

# furrr parallel proc -----------------------------------------------
vec_ids <- unique(sdat$proc_id)
l_ids <- split(vec_ids, cut(seq_along(vec_ids), n_splits, labels = FALSE))

plan(cluster, workers=n_cores)
all_out <- list()
for(i in start_split_idx:n_splits){
  print(paste("starting split ",i));
  gc(full=T)
  system.time(all_out[[i]] <- sdat[proc_id%in%l_ids[[i]]] %>%
                split(f=list(.$id,.$year,.$month),
                  drop=T) %>%
                future_map(~paf_dt_monthly(.x),
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
  "lc-",lc_class,"-max-sif-monthly-fits_",Sys.Date(),".parquet"),
  compression = 'snappy')

