# Description: process by grid loc
# Date: 2020-08-13
# Author: Sami Rifai
# 
# 
# 
pacman::p_load(tidyverse, stars, data.table, lubridate,tidync,tictoc, 
  future.apply)

flist <- list.files("/media/sami/srifai-ssd/data-ssd/tropomi-conus/", 
  pattern = "TROPOMI_2018", 
  full.names = T)
i1 <- read_ncdf(flist[150],var='SIF') %>% 
    st_set_crs(4326)
i2 <- read_ncdf(flist[170],var='SIF') %>% 
    st_set_crs(4326)
i3 <- read_ncdf(flist[190],var='SIF') %>% 
    st_set_crs(4326)

plan(multisession)
tic()
o <- st_apply(c(i1,i2,i3, along='time'),
  MARGIN = c('lon','lat'),
  FUTURE = T,
  FUN=mean,na.rm=T)
toc()
plot(o, col=viridis::inferno(100))


# ref_sif <- stars::read_stars("../data_general/proc_sif-optim/sif_refgrid.tif")
# dref <- ref_sif %>% as.data.table()
dref <- o %>% as.data.table()
dref <- dref %>% rename(x=lon,y=lat)
dref[,`:=`(xc = round(x/4)*4,
           yc = round(y/4)*4)]
dref[,`:=`(xy_id = .GRP, 
           nobs = sum(is.na(mean)==F)), by=.(xc,yc)]
grid <- st_as_stars(dref[,.(x,y,xy_id,nobs)], dims=c("x","y"))
st_crs(grid) <- st_crs(4326)
# plot(grid['nobs'],col=viridis::viridis(100),downsample = F,nbreaks = 101)
# dref <- dref %>% select(-sif_refgrid.tif)
grange <- dref[nobs>0][,.(
  xmin = min(x),
  xmax=max(x),
  ymin=min(y),
  ymax=max(y), 
  nobs = median(nobs)), by=.(xy_id,xc,yc)]

arrow::write_parquet(grange, sink='../data_general/proc_sif-optim/grid-group-sif-4deg')