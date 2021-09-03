# Description: Extract LST from large geotiff stack
# Date: 2020-08-12
# Author: Sami Rifai
# 
# 
# 
pacman::p_load(tidyverse, stars, data.table, lubridate)

ref_sif <- stars::read_stars("../data_general/proc_sif-optim/sif_refgrid.tif")
# plot(ref_sif)

# process 2018 ----------------------------------------------------------
i1 <- stars::read_stars("../data_general/proc_sif-optim/myd11a1_2018-0000000000-0000000000.tif", proxy=F) %>% 
  st_set_dimensions(.,3,values=seq(ymd("2018-01-01"),ymd("2018-12-31"),by='1 day'), 
    names = 'date') %>% 
  set_names("lst")
st_is_longlat(i1)

ref <- st_crop(ref_sif,i1[,,,1],crop =T)
# plot(ref)

i1 <- st_warp(i1,ref,use_gdal = F)
gc(full=T)

fn <- function(i){
  out <- i1[,,,i] %>% 
    as.data.table() %>% 
    .[is.na(lst)==F]
  gc(full=T)
  return(out)
}
di1 <- lapply(1:364,fn)
gc(full=T)
di1 <- rbindlist(di1)
gc(full=T)

arrow::write_parquet(di1,
  sink="../data_general/proc_sif-optim/lst2018_reg1.parquet",
  compression='snappy')

# process 2019 ----------------------------------------------------------
i1 <- stars::read_stars("../data_general/proc_sif-optim/myd11a1_2019-0000000000-0000000000.tif", proxy=F) %>% 
  st_set_dimensions(.,3,values=seq(ymd("2019-01-01"),ymd("2019-12-31"),by='1 day'), 
    names = 'date') %>% 
  set_names("lst")
st_is_longlat(i1)

ref <- st_crop(ref_sif,i1[,,,1],crop =T)
# plot(ref)

i1 <- st_warp(i1,ref,use_gdal = F)
gc(full=T)

fn <- function(i){
  out <- i1[,,,i] %>% 
    as.data.table() %>% 
    .[is.na(lst)==F]
  gc(full=T)
  return(out)
}
di1 <- lapply(1:364,fn)
gc(full=T)
di1 <- rbindlist(di1)
gc(full=T)

arrow::write_parquet(di1,
  sink="../data_general/proc_sif-optim/lst2019_reg1.parquet",
  compression='snappy')
gc()
rm(di1)
gc(full=T)

# process 2020 ----------------------------------------------------------
i1 <- stars::read_stars("../data_general/proc_sif-optim/myd11a1_2020-0000000000-0000000000.tif", proxy=F) %>% 
  st_set_dimensions(.,3,values=seq(ymd("2020-01-01"),ymd("2020-12-31"),by='1 day'), 
    names = 'date') %>% 
  set_names("lst")
st_is_longlat(i1)

ref <- st_crop(ref_sif,i1[,,,1],crop =T)
# plot(ref)

i1 <- st_warp(i1,ref,use_gdal = F)
gc(full=T)

fn <- function(i){
  out <- i1[,,,i] %>% 
    as.data.table() %>% 
    .[is.na(lst)==F]
  gc(full=T)
  return(out)
}
di1 <- lapply(1:364,fn)
gc(full=T)
di1 <- rbindlist(di1)
gc(full=T)

arrow::write_parquet(di1,
  sink="../data_general/proc_sif-optim/lst2020_reg1.parquet",
  compression='snappy')
gc()
rm(di1)
gc(full=T)
