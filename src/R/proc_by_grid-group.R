# Description: process by grid loc
# Date: 2020-08-13
# Author: Sami Rifai
# 
# 
# 
pacman::p_load(tidyverse, stars, data.table, lubridate,tidync,tictoc, 
  future.apply,ncdf4)

# flist <- list.files("/media/sami/srifai-ssd/data-ssd/tropomi-conus/", 
#   pattern = "TROPOMI_2018", 
#   full.names = T)
# i1 <- read_ncdf(flist[150],var='SIF') %>% 
#     st_set_crs(4326)
# i2 <- read_ncdf(flist[170],var='SIF') %>% 
#     st_set_crs(4326)
# i3 <- read_ncdf(flist[190],var='SIF') %>% 
#     st_set_crs(4326)
# o <- st_apply(c(i1,i2,i3),MARGIN = c(1,2),FUN=mean,na.rm=T)
# 
# ref_sif <- stars::read_stars("../data_general/proc_sif-optim/sif_refgrid.tif")
# dref <- ref_sif %>% as.data.table()
# dref[,`:=`(xc = round(x/4)*4,
#            yc = round(y/4)*4)]
# dref[,`:=`(xy_id = .GRP, 
#            nobs = .N), by=.(xc,yc)]
# # grid <- st_as_stars(dref[,.(x,y,xy_id,nobs)], dims=c("x","y"))
# # st_crs(grid) <- st_crs(4326)
# # plot(grid['nobs'])
# # dref <- dref %>% select(-sif_refgrid.tif)
# grange <- dref[,.(
#   xmin = min(x),
#   xmax=max(x),
#   ymin=min(y),
#   ymax=max(y), 
#   nobs = median(nobs)), by=.(xy_id,xc,yc)]
# 
# unique(grange$xy_id) %>% length
# 


# Load xy grid-group and ref sif grid ------------------------------------------
flist <- list.files("/media/sami/srifai-ssd/data-ssd/tropomi-conus/",
  pattern = "TROPOMI_",
  full.names = T)
ref_sif <- stars::read_stars("../data_general/proc_sif-optim/sif_refgrid.tif")
# tmp_x <- stars::read_ncdf(flist[1]) %>% 
#   st_get_dimension_values(.,1)
# tmp_y <- stars::read_ncdf(flist[1]) %>% 
#   st_get_dimension_values(.,2)
# ref_sif <- st_as_stars(expand_grid(x=tmp_x,y=tmp_y),dims=c("x","y"))
# ref_sif['val'] <- rnorm(length(tmp_x)*length(tmp_y))
# st_crs(ref_sif) <- st_crs(4326)

grange <- arrow::read_parquet("../data_general/proc_sif-optim/grid-group-sif-4deg")
grange <- grange[nobs>2000]
i <- grange[1,]$xy_id
xmin <- grange[xy_id==i]$xmin
xmax <- grange[xy_id==i]$xmax
ymin <- grange[xy_id==i]$ymin
ymax <- grange[xy_id==i]$ymax
grange[xy_id==i]

# x_idx <- which(between(st_get_dimension_values(ref_sif,'x'),xmin,xmax))
# y_idx <- which(between(st_get_dimension_values(ref_sif,'y'),ymin,ymax))
ncin <- nc_open(flist[1])
x_idx <- which(between(ncvar_get(ncin,'lon'),xmin,xmax))
y_idx <- which(between(ncvar_get(ncin,'lat'),ymin,ymax))

bb <- sf::st_as_sfc(st_bbox(c(xmin=xmin,ymin=ymin,xmax=xmax,ymax=ymax))) %>% 
  st_set_crs(4326)
ref_sif_crop <- ref_sif %>% st_crop(., bb)

# extract sif -------------------------------------------------------------
fn_extract_sif <- function(fname){
  d <- str_split(fname,"/",simplify = T)[1,8]
  d <- ymd(substr(d,9,16))
  
  # x index starts from western edge
  # y index starts from southern edge
  out <- read_ncdf(fname,var='SIF') %>% 
    st_set_crs(4326) %>% 
  slice(., index=x_idx, along='lon') %>%
  slice(., index=y_idx, along='lat') %>%
  as.data.table() %>% 
  .[,date:=d] %>% 
  .[is.na(SIF)==F]
  gc()
  return(out)
}
fn_extract_sif(flist[180])[]

tic()
plan(multisession)
d_sif <- future_lapply(flist,fn_extract_sif)
gc()
d_sif <- rbindlist(d_sif)
d_sif <- d_sif[is.na(SIF)==F]
gc()
d_sif <- d_sif %>% rename(x=lon,y=lat,sif=SIF)
toc()
gc(full=T)


# LST ---------------------------------------------------------------------
# Scan list of LST tifs to find which ones contain the bounding box
# of the 'grid-group'
mod_list <- list.files("../data_general/proc_sif-optim",pattern = 'myd11a1',
  full.names = T)
mod_list <- mod_list[!str_detect(mod_list,'refgrid')] # remove ref grid
fn_find_tif <- function(fname){
  f_bb <- stars::read_stars(fname,proxy=T) %>% 
    st_bbox() %>% 
    st_as_sfc() %>% 
    st_set_crs(4326)
  if((st_intersection(bb,f_bb) %>% length) >0){
    out <- fname
  } else { out <- NA}
  return(out)
}
plan(multisession)
ml2 <- future_lapply(mod_list,fn_find_tif)
ml2 <- ml2[!is.na(ml2)]
ml2 <- unlist(ml2)
gc(full=T)

fn_extract_lst <- function(fname){
  im_year <- str_extract(ml2[1],"(\\d{4})") # (:capture \\d{4}:4numbers ):capture
  tmp <- stars::read_stars(fname,proxy=T) %>% 
    st_as_stars() %>% 
    set_names('lst')

  tmp <- tmp %>% st_crop(., bb,crop=T)
  tmp <- tmp %>% st_warp(., ref_sif_crop, use_gdal = F)
  n_days <- dim(tmp)[3]
  start_date <- ymd(paste(im_year,1,1))
  end_date <- ymd(paste(im_year,1,1))+days(n_days-1)
  vec_dates <- seq(start_date,end_date,by='1 day')
  tmp <- tmp %>% 
      st_set_dimensions(.,3,
      values=vec_dates,
    names = 'date')
  out <- tmp %>% as.data.table()
  gc()
  return(out)
}

plan(multisession)
d_lst <- future_lapply(ml2,fn_extract_lst)
d_lst <- rbindlist(d_lst)
gc(full=T)


# merge SIF & LST ---------------------------------------------------------
gc(full=T)
d_sif <- d_sif[,`:=`(x=round(x,5),
            y=round(y,5))]
d_lst <- d_lst[,`:=`(x=round(x,5),
            y=round(y,5))]
gc(full=T)
setkeyv(d_sif,c("x","y","date"));gc()
setkeyv(d_lst,c("x","y","date"));gc()
gc(full=T)
dat <- merge(d_sif,d_lst,by=c("x","y","date"))



arrow::write_parquet(dat[is.na(lst)==F],
  sink=paste0("../data_general/proc_sif-optim/merged_parquets/","sif-lst_xy-id-",grange[ng,]$xy_id,".parquet"), 
  compression='snappy')
# unique(d_sif$x) %in% unique(d_lst$x)
# unique(d_sif$y) %in% unique(d_lst$y)
# unique(d_sif$date) %in% unique(d_lst$date)

# # SCRAP ************************************************************************
dat[sample(.N,1e6)] %>% 
  .[,`:=`(month=month(date))] %>% 
  ggplot(data=.,aes(lst,sif))+
  geom_smooth()+
  facet_wrap(~month)


# tmp <- read_ncdf(flist[180],var='SIF') %>% 
#   slice(., index=x_idx, along='lon') %>% 
#   slice(., index=y_idx, along='lat') %>% 
#   as.data.table()
# tmp
# 
# junk <- read_ncdf(flist[180],
#   var='SIF',make_time=F,make_units = F,
#   ncsub = cbind(start = c(min(x_idx), min(y_idx)), 
#     count = c(max(x_idx), max(y_idx))),
#   ignore_bounds = T)
# junk %>% 
#   as_tibble() %>% filter(is.na(SIF)==F)
# 
# read_ncdf(flist[10],
#   var='SIF',
#   ncsub = cbind(start = c(min(x_idx), min(y_idx)), 
#     count = c(max(x_idx), max(y_idx)))) %>% 
#   as.data.table() %>% 
#   rename(x=lon,y=lat,sif=SIF) %>% 
#   select(x,y,sif) %>% 
#   .[is.na(sif)==F]
# 
# read_ncdf(flist[1], var='SIF',
#   ncsub = cbind(start = c(1, 1), count = c(10, 12))
#   )
# tidync(flist[1])
# 
# bb <- st_bbox(c(xmin=xmin,xmax=xmax,ymax=ymax,ymin=ymin),crs=st_crs(4326))
# sf::st_bbox(c(xmin=xmin,xmax=xmax,ymax=ymax,ymin=ymin),crs=st_crs(4326))
# st_polygon(
#   matrix(c(c(xmin,ymin), 
#          c(xmax,ymax),
#          c(xmin,ymax),
#          c(xmax,ymin)),
#   ncol=2,
#   byrow = T))
# bb <- st_polygon(list(cbind(c(xmin,xmin,xmax,xmax,xmin),
#                        c(ymin,ymax,ymax,ymin,ymin))))
# st_as_sf((cbind(c(xmin,xmin,xmax,xmax,xmin),
#                        c(ymin,ymax,ymax,ymin,ymin))))
# st_crs(bb) <- st_crs(4326)
# bb <- st_sfc(bb,crs=4326)
# 
# tmp <- stars::read_ncdf(flist[1],var='SIF',
#   # curvilinear = c('lon','lat'),
#   ignore_bounds = T) %>% 
#   st_set_crs(4326)
# 
# st_crs(tmp) <- st_crs(4326)
# tmp[bb]
# stars:::xy_from_colrow(tmp)
# st_is_longlat(bb)
# 
# fn_extract_sif <- function(fname){
#   d <- str_split(fname,"/",simplify = T)[1,8]
#   d <- ymd(substr(d,9,16))
#   out <- tidync(fname) %>% 
#   activate(what = 'SIF') %>% 
#   hyper_filter(lon = lon <= xmax) %>% 
#   hyper_filter(lon = lon >= xmin) %>% 
#   # hyper_filter(lat = lat >= ymin) %>%  
#   # hyper_filter(lat = lat <= ymax) %>% 
#   hyper_tibble() %>%
#   as.data.table() %>% 
#     .[,date:=d]
#   gc()
#   return(out)
# }
# 
# tmp1 <- fn_extract_sif(flist[2])
# tmp1$lon %>% summary
# 
# 
# 
# # arrow::write_parquet(d2018[,.(x,y,date,sif)],
# #   "../data_general/proc_sif-optim/sif2018_reg1.parquet", 
# #   compression='snappy')
# # rm(dat); gc(full=T)
