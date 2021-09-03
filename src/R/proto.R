# Description: plot GAM SIF~LST
# Date: 2020-08-13
# Author: Sami Rifai
# 
# 
# 
pacman::p_load(tidyverse, stars, data.table, lubridate, patchwork)

gc(full=T)
sif <- arrow::read_parquet("../data_general/proc_sif-optim/sif2018_reg1.parquet")
gc(full=T)
di1 <- arrow::read_parquet("../data_general/proc_sif-optim/lst2018_reg1.parquet")
gc(full=T)


tmp1 <- sif[between(x,-123.5,-122.5)][between(y,44,45)]
tmp2 <- di1[between(x,-123.5,-122.5)][between(y,44,45)]
tmp1[,`:=`(x=round(x,5),
  y=round(y,5))]
tmp2[,`:=`(x=round(x,5),
  y=round(y,5))]

tmp3 <- merge(tmp1,tmp2,by=c("x","y","date"))

p1 <- tmp3[,.(val = median(sif,na.rm=T)),by=.(x,y)] %>% 
  ggplot(data=.,aes(x,y,fill=val))+
  geom_raster()+
  coord_sf(expand = F)+
  scale_fill_viridis_c()+
  labs(x=NULL,y=NULL,fill='SIF',title='Median TROPOMI SIF (2018)')+
  theme_linedraw(); p1

p2 <- tmp3[date>ymd("2018-02-01")][date<ymd("2018-12-31")][,month:=month(date)][sample(.N,100000)] %>% 
  ggplot(data=.,aes(lst,sif,color=factor(month)))+
  # geom_point()+
  geom_smooth(method='gam',
    formula=y~s(x,bs='cs',k=5),
    se=F
    # color="#cf0000"
    )+
  scale_color_viridis_d()+
  theme_linedraw()+
  labs(x='Land Skin Temperature (K)',y='SIF',color='month', 
    title='Monthly SIF (2018)')+
  theme(panel.grid = element_blank());p2
  # facet_wrap(~month,ncol = 1,scales = 'free_y',labeller = label_both); p2

ggsave(p1|p2,filename = 'figures/proto_med-sif_sif-lst-gam-month.png',
  device=grDevices::png,
  width=200,
  height=110,
  units='mm',
  dpi=350)



# unique(tmp1$date) %in% unique(tmp2$date)
# unique(tmp1$x) %in% unique(tmp2$x)
# unique(tmp1$y) %in% unique(tmp2$y)
# tmp3
# 
# cbind(sort(unique(tmp1$x)),sort(unique(tmp2$x))) %>% 
#   round(digits = 5)
# 
# 
# 
# ggplot(data=tmp2[date==ymd("2018-08-01")], 
#   aes(x,y,fill=lst))+
#   geom_raster()+
#   coord_equal()+
#   scale_fill_viridis_c(
#     #limits=c(273.15,300),
#     oob=scales::squish)
smap <- stars::read_stars("../data_general/proc_sif-optim/smap_ssma_2018.tif",
  proxy=T) %>% 
  st_set_dimensions(.,3,values=seq(ymd("2018-01-01"),ymd("2018-12-31"),length.out=122), 
    names = 'date') %>% 
  set_names("ssma")

smap[,,,1] %>% plot(., col=viridis::viridis(10),breaks='equal', 
  downsample=F)
