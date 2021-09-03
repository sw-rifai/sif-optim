pacman::p_load(tidyverse, stars, data.table, lubridate,tidync,tictoc, 
  future.apply,ncdf4)

# Load xy grid-group and ref sif grid ------------------------------------------
flist <- list.files("/media/sami/srifai-ssd/data-ssd/tropomi-conus/",
  pattern = "TROPOMI_",
  full.names = T)
ref_sif <- stars::read_stars("../data_general/proc_sif-optim/sif_refgrid.tif")

grange <- arrow::read_parquet("../data_general/proc_sif-optim/grid-group-sif-4deg")
grange <- grange[nobs>2000]


# MASSIVE PROCESSING LOOP ------------------------------------------------------
# VERY memory hungry
for(ng in 3:dim(grange)[1]){
  print(paste0("processing: ",ng," out of ",dim(grange)[1]))

  i <- grange[ng,]$xy_id
  xmin <- grange[xy_id==i]$xmin
  xmax <- grange[xy_id==i]$xmax
  ymin <- grange[xy_id==i]$ymin
  ymax <- grange[xy_id==i]$ymax
  grange[xy_id==i]
  
  ncin <- nc_open(flist[1])
  x_idx <- which(between(ncvar_get(ncin,'lon'),xmin,xmax))
  y_idx <- which(between(ncvar_get(ncin,'lat'),ymin,ymax))
  nc_close(ncin)
  rm(ncin)
  
  bb <- sf::st_as_sfc(st_bbox(c(xmin=xmin,ymin=ymin,xmax=xmax,ymax=ymax))) %>% 
    st_set_crs(4326)
  ref_sif_crop <- ref_sif %>% st_crop(., bb)
  
  # extract sif -------------------------------------------------------------
  print('extracting SIF')
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
  # fn_extract_sif(flist[180])[]
  
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
  print('extracting LST')
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
    im_year <- str_extract(fname,"(\\d{4})") # (:capture \\d{4}:4numbers ):capture
    tmp <- stars::read_stars(fname,proxy=T) %>% 
      st_as_stars() %>% 
      set_names('lst')
  
    tmp <- tmp %>% st_crop(., bb,crop=T)
    gc()
    tmp <- tmp %>% st_warp(., ref_sif_crop, use_gdal = F)
    gc()
    n_days <- dim(tmp)[3]
    start_date <- ymd(paste(im_year,1,1))
    end_date <- ymd(paste(im_year,1,1))+days(n_days-1)
    vec_dates <- seq(start_date,end_date,by='1 day')
    tmp <- tmp %>% 
        st_set_dimensions(.,3,
        values=vec_dates,
      names = 'date')
    out <- tmp %>% as.data.table() %>% 
      .[is.na(lst)==F]
    gc(full=T)
    return(out)
  }
  
  plan(sequential)
  d_lst <- future_lapply(ml2,fn_extract_lst)
  d_lst <- rbindlist(d_lst)
  gc(full=T)
  plan(sequential)
  
  
  # merge SIF & LST ---------------------------------------------------------
  print('merging')
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
  gc(full=T)
  
  
  print("writing parquet")
  arrow::write_parquet(dat[is.na(lst)==F],
    sink=paste0("../data_general/proc_sif-optim/merged_parquets/","sif-lst_xy-id-",grange[ng,]$xy_id,".parquet"), 
    compression='snappy')
  
  # cleanup -----------------------------------------------------------------
  rm(dat,d_sif,d_lst)
  rm(ref_sif_crop)
  rm(bb, x_idx,y_idx,xmin,xmax,ymin,ymax)
  gc(full=T)
}
