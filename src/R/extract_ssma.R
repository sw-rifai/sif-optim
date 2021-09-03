pacman::p_load(tidyverse, stars, data.table, lubridate,tictoc, 
  future.apply)

dat <- arrow::read_parquet("../data_general/proc_sif-optim/merged_parquets/sif-lst_xy-id-103.parquet")

xy <- unique(dat[,.(x,y)])
xy_sf <- sf::st_as_sf(xy, coords=c("x","y"),crs=4326)

tmp <- stars::read_stars("../data_general/proc_sif-optim/smap_ssma_2018.tif") %>% 
  st_set_dimensions(., 3, 
    values=seq(ymd("2018-01-01"),ymd("2018-12-30"),length.out=122), 
    name='time')
names(tmp) <- 'ssma'

ref_sif <- stars::read_stars("../data_general/proc_sif-optim/sif_refgrid.tif")
lc <- stars::read_stars("../data_general/proc_sif-optim/mcd12q1_igbp-land-cover_2019.tif",
  proxy=F)
names(lc) <- 'lc'

bb <- sf::st_as_sfc(st_bbox(
  c(xmin=min(xy$x),
    ymin=min(xy$y),
    xmax=max(xy$x),
    ymax=max(xy$y)))) %>% 
    st_set_crs(4326)

tmp <- tmp %>% st_crop(., bb)
ref_sif <- ref_sif %>% st_crop(., bb)
lc <- lc %>% st_crop(., bb)
tmp <- st_warp(tmp,ref_sif,use_gdal = F)
lc <- st_warp(lc,ref_sif,use_gdal = F)

dlc <- lc %>% as.data.table()
dlc <- dlc[,`:=`(x=round(x,5),
  y=round(y,5))]
setkeyv(dlc,c("x","y"))
d1 <- tmp %>% as.data.table()
d1 <- d1 %>% rename(date=time)
d1 <- d1[,`:=`(x=round(x,5),
  y=round(y,5))]
setkeyv(d1,c("x","y","date"))
d2 <- d1[dat,roll='nearest']
d2 <- d2[is.na(ssma)==F]
d2 <- dlc[d2]
d2 <- d2[is.na(lc)==F][is.na(sif)==F][is.na(ssma)==F]
d2[,month:=month(date)]


d2[sample(.N,10000)] %>% 
  .[,month:=month(date)] %>% 
  ggplot(data=.,aes(x,y,color=ssma))+
  geom_point()+
  scale_color_gradient2()+
  facet_wrap(~month)

library(mgcv)
d2[between(month,7,7)] %>%
  .[sample(1e6)] %>% 
  ggplot(data=.,aes(lst,sif))+
  # geom_point()+
  geom_smooth(method='bam',
    formula=y~s(x,bs='cs',k=6),
    method.args=list(discrete=T))+
  facet_grid(cut_number(ssma,5)~lc,
    scales='free_y')

