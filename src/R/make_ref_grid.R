pacman::p_load(tidyverse, stars, data.table, lubridate)

# SIF ref grid ---------------------------------------------------------------
ref_sif <- stars::read_ncdf("/media/sami/srifai-ssd/data-ssd/tropomi-conus/TROPOMI_20180301.nc", var='SIF')
st_crs(ref_sif) <- st_crs(4326)
ref_sif <- ref_sif %>% 
  as.data.table()
ref_sif <- ref_sif %>% rename(x=lon,y=lat)
ref_sif <- st_as_stars(ref_sif,dims=c('x','y'),xy = c("x","y"))
st_crs(ref_sif) <- st_crs(4326)
st_is_longlat(ref_sif)
stars::write_stars(ref_sif, dsn = "../data_general/proc_sif-optim/sif_refgrid.tif")
plot(ref_sif)



i1 <- stars::read_stars("../data_general/proc_sif-optim/myd11a1_2018-0000000000-0000000000.tif", proxy=F) %>% 
  st_set_dimensions(.,3,values=seq(ymd("2018-01-01"),ymd("2018-12-31"),by='1 day'), 
    names = 'date') %>% 
  set_names("lst")
st_is_longlat(i1)

ref <- st_crop(ref_sif,i1[,,,1],crop =T)
plot(ref)

i1 <- st_warp(i1,ref,use_gdal = F)


# LST ref grid ---------------------------------------------------------------
flist <- list.files("../data_general/proc_sif-optim/",
  pattern = 'myd11a1_2018',
  full.names = T)
flist

fn <- function(fname){
  o <- stars::read_stars(fname,proxy = F,RasterIO = list(bands=1))
  return(o)
}

tmp <- lapply(flist, fn)

ref <- st_mosaic(tmp[[1]],tmp[[2]],tmp[[3]],tmp[[4]],tmp[[5]],
  tmp[[6]],tmp[[7]],tmp[[8]],tmp[[9]],tmp[[10]],
  tmp[[11]],tmp[[12]],tmp[[13]],tmp[[14]],tmp[[15]])
stars::write_stars(ref, "../data_general/proc_sif-optim/myd11a1_refgrid.tif")

# plot(ref,breaks='equal')
# 
# 
# names(tmp) <- list.files("../data_general/proc_sif-optim/",
#   pattern = 'myd11a1_2018',
#   full.names = F)
# names(tmp)
# 
# st_mosaic("tmp[[1]]","tmp[[2]]")
# st_mosaic(tmp[[1]],tmp[[2]],tmp[[3]])
# unlist(tmp)
# ref <- st_mosaic(unstack(tmp))
# st_as_stars(tmp)
# 
# 
# stars::read_stars(flist[1],proxy = T,RasterIO = list(bands=c(1)))
# 
# o <- stars::read_stars(flist,proxy = F,RasterIO = list(bands=1))
# st_as_stars(tmp)
# 
