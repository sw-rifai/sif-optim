# Description: Extract SIF from large CONUS NC collection
# Date: 2020-08-12
# Author: Sami Rifai
# 

library(tidyverse); 
library(stars)
library(data.table)
library(lubridate)
library(tidync)

fn <- function(fname){
  d <- str_split(fname,"/",simplify = T)[1,8]
  d <- ymd(substr(d,9,16))
  out <- tidync(fname) %>% 
  activate() %>% 
  hyper_filter(lon = lon < -120) %>% 
  hyper_filter(lat = lat > 40) %>% 
  hyper_tibble() %>%
  as.data.table() %>% 
    .[,date:=d]
  return(out)
}
flist <- list.files("/media/sami/srifai-ssd/data-ssd/tropomi-conus/", 
  pattern = "TROPOMI_2018", 
  full.names = T)

system.time(dat <- lapply(flist,fn))
gc()
dat <- rbindlist(dat)
gc()
dat <- dat %>% rename(x=lon,y=lat,sif=SIF)
arrow::write_parquet(dat[,.(x,y,date,sif)],
  "../data_general/proc_sif-optim/sif2018_reg1.parquet", 
  compression='snappy')
rm(dat); gc(full=T)


# Proc 2019 ---------------------------------------------------------------
flist <- list.files("/media/sami/srifai-ssd/data-ssd/tropomi-conus/", 
  pattern = "TROPOMI_2019", 
  full.names = T)

system.time(dat <- lapply(flist,fn))
gc()
dat <- rbindlist(dat)
gc()
dat <- dat %>% rename(x=lon,y=lat,sif=SIF)
arrow::write_parquet(dat[,.(x,y,date,sif)],
  "../data_general/proc_sif-optim/sif2019_reg1.parquet", 
  compression='snappy')
rm(dat); gc(full=T)


# Proc 2020 ---------------------------------------------------------------
flist <- list.files("/media/sami/srifai-ssd/data-ssd/tropomi-conus/", 
  pattern = "TROPOMI_2020", 
  full.names = T)

system.time(dat <- lapply(flist,fn))
gc()
dat <- rbindlist(dat)
gc()
dat <- dat %>% rename(x=lon,y=lat,sif=SIF)
arrow::write_parquet(dat[,.(x,y,date,sif)],
  "../data_general/proc_sif-optim/sif2020_reg1.parquet", 
  compression='snappy')
rm(dat); gc(full=T)





# ref <- stars::read_stars("../data_general/proc_sif-optim/myd11a1_refgrid.tif")
# ref <- ref %>% 
#   set_names("lst")
# ref <- ref %>% as.data.table()
# pts <- ref[is.na(lst)==F][sample(.N, 100)] %>% 
#   st_as_sf(., coords=c("x","y"), 
#     crs=st_crs(4326))
# 
# hf <- tidync("/media/sami/srifai-ssd/data-ssd/tropomi-conus/TROPOMI_20180202.nc") %>% 
#   activate() %>% 
#   hyper_filter(lon = lon < -120) %>% 
#   hyper_filter(lat = lat > 40)
# ht <- hf %>% hyper_tibble()
# 
# ht %>% 
#   ggplot(data=.,aes(lon,lat,fill=SIF))+
#   geom_raster()+
#   coord_sf()+
#   scale_fill_viridis_c(limits=c(0,2.5))
# 
# dev.new()
# tidync("/media/sami/srifai-ssd/data-ssd/tropomi-conus/TROPOMI_20180203.nc") %>% 
#   activate() %>% 
#   hyper_filter(lon = lon < -120) %>% 
#   hyper_filter(lat = lat > 40) %>% 
#   hyper_tibble() %>% 
#   ggplot(data=.,aes(lon,lat,fill=SIF))+
#   geom_raster()+
#   coord_sf()+
#   scale_fill_viridis_c(limits=c(0,2.5))
# 
# 
# fn <- function(fname){
#   d <- str_split(fname,"/",simplify = T)[1,8]
#   d <- ymd(substr(d,9,16))
#   out <- tidync(fname) %>% 
#   activate() %>% 
#   hyper_filter(lon = lon < -120) %>% 
#   hyper_filter(lat = lat > 40) %>% 
#   hyper_tibble() %>%
#   as.data.table() %>% 
#     .[,date:=d]
#   return(out)
# }
# flist <- list.files("/media/sami/srifai-ssd/data-ssd/tropomi-conus/", 
#   pattern = "TROPOMI_2018", 
#   full.names = T)
# str_split(flist[1],"/",simplify = T)[1,8]
# fn(flist[1])[]
# 
# dat <- lapply(flist,fn)
# gc()
# dat <- rbindlist(dat)
# dim(dat)
# 
# s <- dat[between(lon,-123.5,-123) & between(lat,44,45)][
#   ,`:=`(month=month(date)][,
#     .(val=mean(SIF,na.rm=T)),by=.(x,y,month)]
# 
# dat %>% 
#     ggplot(data=.,aes(lon,lat,fill=SIF))+
#   geom_raster()+
#   coord_sf()+
#   scale_fill_viridis_c(limits=c(0,2.5))+
#   facet_wrap(~date)
# 
# 
# ymd(substr(flist[1],9,16))
# 
# vals <- gdalio_data("/media/sami/srifai-ssd/data-ssd/tropomi-conus/TROPOMI_20180202.nc", 
#   bands=1L)
# vals
# 
# o <- terra::rast("/media/sami/srifai-ssd/data-ssd/tropomi-conus/TROPOMI_20180201.nc")
# system.time(
#   v <- terra::extract(o, sf::st_coordinates(pts))
# )
# terra::as.data.frame(o)
# 
# o <- stars::read_ncdf("/media/sami/srifai-ssd/data-ssd/tropomi-conus/TROPOMI_20180201.nc",var='SIF')
#   # make_time = T, make_units = F, 
#   ncsub = cbind(start = c(1, 1), count = c(2000, 2000)))
# st_crs(o) <- st_crs(4326)
# o
# plot(o)
# 
# ggplot()+
#   geom_stars(data=o[1:1000,1:1000], 
#     aes(lon,lat,fill=SIF))+
#   scale_fill_viridis_c(limits=c(0,2.5))
# 
# 
# st_crs(o) <- st_crs(4326)
# 
# s <- st_extract(o, pts)
# bind_cols(pts,st_drop_geometry(s)) %>% 
#   filter(SIF<2.5) %>% 
#   filter(SIF>0) %>% 
#   filter(lst>273) %>% 
#   ggplot(data=.,aes(lst,SIF))+
#   geom_point()+
#   geom_smooth()
# 
# 
# aq <- stars::read_stars("../data_general/proc_sif-optim/myd11a1_2018-0000000000-0000000000.tif",proxy = F)
# names(aq) <- 'lst'
# aq <- aq %>% 
#   st_set_dimensions(., 3, 
#     values=seq(ymd("2018-01-01"),ymd("2018-12-30"),by='1 day'), 
#     names = 'date')
# aq[,,,100] %>% plot(., col=viridis::inferno(100),breaks='equal')
# 
# st_bbox(aq)
# 
# c(1,2,3,NA) %>% max(na.rm=T)
# c(NA,NA) %>% pmax(na.rm=T)
# all.equal(c(NA,NA,1))
# fn_max <- function(x){
#   out <- max(x,na.rm=T)
#   if(is.infinite(out)==T){out <- NA_real_}
#   return(out)
# }
# aq_mx <- st_apply(aq,MARGIN = c("x","y"),FUN=fn_max)
# plot(aq_mx, col=viridis::inferno(100),breaks='equal')
# 
# o
# 
# o <- stars::read_stars("/media/sami/srifai-ssd/data-ssd/tropomi-conus/TROPOMI_20180201.nc",proxy=T)["SIF"]
# st_crs(o) <- st_crs(4326)
# st_warp(o,ref)
# o <- st_crop(o,y = st_bbox(aq))
# plot(o,col=viridis::viridis(100),breaks='equal')
# 
# sif <- st_as_stars(o)
# 
# 
# 
# o <- stars::read_ncdf("/media/sami/srifai-ssd/data-ssd/tropomi-conus/TROPOMI_20180201.nc",var='SIF',make_units = F)
# st_crs(o) <- st_crs(4326)
# o <- o[,1:1000]
# o <- st_warp(o, ref,use_gdal = F)
# o
# 
# 
# o[,1:1500] %>% st_get_dimension_values(.,'lon') %>% max
# st_crop(o[,1:1500],st_bbox(aq_mx))
# 
# plot(o[,1:1000,],breaks='equal',col=viridis::viridis(100))
# o <- st_crop(o,aq)
# 
# ggplot()+
#   geom_stars(data=o[,2000:6000,1000:5000])+
#   scale_fill_viridis_c(limits=c(0,10))+
#   coord_sf()
