# Description: Plot
# Notes: 
# Always use the most recent model fits per landclass
# Filter out poor fits

# US National LC class description ---------------------------------
# // Export SIF and LST by land-cover class

# // Val Color	Description
# // 41	68ab5f	Deciduous forest: areas dominated by trees generally greater than 5 meters tall, and greater than 20% of total vegetation cover. More than 75% of the tree species shed foliage simultaneously in response to seasonal change.
# // 42	1c5f2c	Evergreen forest: areas dominated by trees generally greater than 5 meters tall, and greater than 20% of total vegetation cover. More than 75% of the tree species maintain their leaves all year. Canopy is never without green foliage.
# // 43	b5c58f	Mixed forest: areas dominated by trees generally greater than 5 meters tall, and greater than 20% of total vegetation cover. Neither deciduous nor evergreen species are greater than 75% of total tree cover.
# // 51	af963c	Dwarf scrub: Alaska only areas dominated by shrubs less than 20 centimeters tall with shrub canopy typically greater than 20% of total vegetation. This type is often co-associated with grasses, sedges, herbs, and non-vascular vegetation.
# // 52	ccb879	Shrub/scrub: areas dominated by shrubs less than 5 meters tall with shrub canopy typically greater than 20% of total vegetation. This class includes true shrubs, young trees in an early successional stage, or trees stunted from environmental conditions.
# // 71	dfdfc2	Grassland/herbaceous: areas dominated by gramanoid or herbaceous vegetation, generally greater than 80% of total vegetation. These areas are not subject to intensive management such as tilling, but can be utilized for grazing.
# // 81	dcd939	Pasture/hay: areas of grasses, legumes, or grass-legume mixtures planted for livestock grazing or the production of seed or hay crops, typically on a perennial cycle. Pasture/hay vegetation accounts for greater than 20% of total vegetation.
# // 82	ab6c28	Cultivated crops: areas used for the production of annual crops, such as corn, soybeans, vegetables, tobacco, and cotton, and also perennial woody crops such as orchards and vineyards. Crop vegetation accounts for greater than 20% of total vegetation. This class also includes all land being actively tilled.
# // 90	b8d9eb	Woody wetlands: areas where forest or shrubland vegetation accounts for greater than 20% of vegetative cover and the soil or substrate is periodically saturated with or covered with water.
# // 95	6c9fb8	Emergent herbaceous wetlands: areas where perennial herbaceous vegetation accounts for greater than 80% of vegetative cover and the soil or substrate is periodically saturated with or covered with water.

pacman::p_load(tidyverse,stars,data.table,mgcv,nls.multstart,arrow,lubridate, 
  patchwork)
setwd("/home/sami/srifai@gmail.com/work/research/sif-optim/")
print(getwd())
source("src/R/functions_sif.R")
 

# prep dat ----------------------------------------------------------------
d41 <- arrow::read_parquet(paste0("../data_general/proc_sif-optim/parquet-by-lc/sif_lst_pdsi_lc",41,".parquet")) %>% .[,fmonth := month(date,label = T)]

sd41 <- d41[,`:=`(clst = round(lst,1))][,.(lst = mean(clst,na.rm=T),
                pdsi = mean(pdsi,na.rm=T)), 
  by=.(id,year,month)]

fname <- list.files("../data_general/proc_sif-optim/mod_fits_max_sif_monthly/",
  pattern=paste0("lc-",41,"-max-sif-monthly-fits_\\d{4}"), 
  full.names = T) 
f41 <- arrow::read_parquet(fname)
setDT(f41)
f41[,.(val=sum(term=='Topt')),by=month]

f41 <- merge(sd41, f41,
  by=c("id","year","month"))


d42 <- arrow::read_parquet(paste0("../data_general/proc_sif-optim/parquet-by-lc/sif_lst_pdsi_lc",42,".parquet")) %>% .[,fmonth := month(date,label = T)]
sd42 <- d42[,`:=`(clst = round(lst,1))][,.(lst = mean(clst,na.rm=T),
                pdsi = mean(pdsi,na.rm=T)), 
  by=.(id,year,month)]
fname <- list.files("../data_general/proc_sif-optim/mod_fits_max_sif_monthly/",
  pattern=paste0("lc-",42,"-max-sif-monthly-fits_\\d{4}"), 
  full.names = T) 
f42 <- arrow::read_parquet(fname)
setDT(f42)
f42[,.(val=sum(term=='Topt')),by=month]
f42 <- merge(sd42, f42,
  by=c("id","year","month"))

d43 <- arrow::read_parquet(paste0("../data_general/proc_sif-optim/parquet-by-lc/sif_lst_pdsi_lc",43,".parquet")) %>% .[,fmonth := month(date,label = T)]
sd43 <- d43[,`:=`(clst = round(lst,1))][,.(lst = mean(clst,na.rm=T),
                pdsi = mean(pdsi,na.rm=T)), 
  by=.(id,year,month)]
fname <- list.files("../data_general/proc_sif-optim/mod_fits_max_sif_monthly/",
  pattern=paste0("lc-",43,"-max-sif-monthly-fits_\\d{4}"), 
  full.names = T) 
f43 <- arrow::read_parquet(fname)
setDT(f43)
f43[,.(val=sum(term=='Topt')),by=month]
f43 <- merge(sd43, f43,
  by=c("id","year","month"))

gc(full=T)
f41[,`:=`(lc=41)]
f42[,`:=`(lc=42)]
f43[,`:=`(lc=43)]

fits <- rbindlist(list(f41,f42,f43))

tc <- stars::read_stars(
  list.files("../data_general/proc_sif-optim/conus_terraclim/",'tif',
    full.names = T))
names(tc) <- str_remove(names(tc),".tif") %>% str_remove(., "terraclimate_")
ref_x <- seq(from=range(fits$xc)[1],to=range(fits$xc)[2],by=0.25)
ref_y <- seq(from=range(fits$yc)[1],to=range(fits$yc)[2],by=0.25)
ref <- st_as_stars(expand_grid(x=ref_x,y=ref_y),crs=st_crs(4326))
tc <- st_warp(tc,ref,use_gdal = F)
tc <- tc %>% as.data.table()
tc <- tc[is.na(map)==F]

fits <- merge(fits,tc,by.x=c("xc","yc"),by.y=c("x","y"))

dem <- stars::read_stars(list.files("../data_general/proc_sif-optim/",'dem',
    full.names = T)) %>% 
  set_names('elevation')
dem <- st_warp(dem,ref,use_gdal = F)
dem <- dem %>% as.data.table()
setnames(dem, c("x","y"),new=c("xc","yc"))
fits <- merge(fits, dem, by=c("xc","yc"))


d1 <- stars::read_stars(
  list.files("../data_general/proc_sif-optim/conus_daymet/",'2018',
    full.names = T)) %>% 
  st_set_dimensions(.,3, 
 values=seq(ymd("2018-05-01"),ymd("2018-09-29"),by='1 day'),names = 'date') %>% 
  set_names('tmax')
d2 <- stars::read_stars(
  list.files("../data_general/proc_sif-optim/conus_daymet/",'2019',
    full.names = T)) %>% 
    st_set_dimensions(.,3, 
 values=seq(ymd("2019-05-01"),ymd("2019-09-29"),by='1 day'),names = 'date') %>% 
  set_names('tmax')
d3 <- stars::read_stars(
  list.files("../data_general/proc_sif-optim/conus_daymet/",'2020',
    full.names = T)) %>% 
  st_set_dimensions(.,3, 
 values=seq(ymd("2020-05-01"),ymd("2020-06-30"),by='1 day'),names = 'date') %>% 
  set_names('tmax')


ref_x <- seq(from=range(fits$xc)[1],to=range(fits$xc)[2],by=0.25)
ref_y <- seq(from=range(fits$yc)[1],to=range(fits$yc)[2],by=0.25)
ref <- st_as_stars(expand_grid(x=ref_x,y=ref_y),crs=st_crs(4326))
d1 <- st_warp(d1,ref,use_gdal = F)
d2 <- st_warp(d2,ref,use_gdal = F)
d3 <- st_warp(d3,ref,use_gdal = F)
d1 <- d1 %>% as.data.table()
d2 <- d2 %>% as.data.table()
d3 <- d3 %>% as.data.table()
daymet <- rbindlist(list(d1,d2,d3))
rm(d1,d2,d3)
gc(full=T)

daymet <- daymet[is.na(tmax)==F]
setnames(daymet,'tmax','tamax')
setnames(daymet,c("x","y"),c("xc","yc"))
setkeyv(daymet,cols = c("xc","yc","date"))
d41 <- merge(d41,daymet,by=c('xc','yc','date'))
d42 <- merge(d42,daymet,by=c('xc','yc','date'))
d43 <- merge(d43,daymet,by=c('xc','yc','date'))
gc(full=T)

l1 <- stars::read_stars(
  list.files("../data_general/proc_sif-optim/conus_lai/",'2018',
    full.names = T)[1], proxy = T) %>% 
  st_set_dimensions(.,3, 
 values=seq(ymd("2018-05-01"),ymd("2018-09-29"),by='4 days'),names = 'date') %>% 
  set_names("lai") %>% 
  as.data.table() %>% 
  .[lai>0]
gc(full=T)

l2 <- stars::read_stars(
  list.files("../data_general/proc_sif-optim/conus_lai/",'2018',
    full.names = T)[2], proxy = T) %>% 
  st_set_dimensions(.,3, 
 values=seq(ymd("2018-05-01"),ymd("2018-09-29"),by='4 days'),names = 'date') %>% 
  set_names("lai") %>% 
  as.data.table() %>% 
  .[lai>0]
gc(full=T)

l3 <- stars::read_stars(
  list.files("../data_general/proc_sif-optim/conus_lai/",'2018',
    full.names = T)[3], proxy = T) %>% 
  st_set_dimensions(.,3, 
 values=seq(ymd("2018-05-01"),ymd("2018-09-29"),by='4 days'),names = 'date') %>% 
  set_names("lai") %>% 
  as.data.table() %>% 
  .[lai>0]
gc(full=T)

l1 <- rbindlist(list(l1,l2,l3))
rm(l2,l3); gc(full=T)

setkeyv(l1,cols=c("x","y","date"))
gc(full=T)

d41_1 <- d41[year==2018]
setkeyv(d41_1,cols=c("x","y","date"))
d41_1 <- l1[d41_1,roll='nearest']
gc(full=T)

s_lai <- l1[,`:=`(xc=round(x*4)/4,
  yc=round(y*4)/4,
  year=year(date),
  month=month(date))][,
    .(lai_mean = mean(lai*0.1,na.rm=T)),by=.(xc,yc,year,month)]
gc(full=T)


# Get name of LC ------------------------------------------------------
# lc_name <- case_when(lc_class==41~"Decid. Forest", 
#                    lc_class==42~"Evergreen Forest",
#                    lc_class==43~"Mixed Forest",
#                    lc_class==90~"Woody Wetlands") 
fits <- fits[,`:=`(lc_class = case_when(lc==41~"Decid. Forest",
                                lc==42~"Evergreen Forest", 
                                lc==43~"Mixed Forest"))]

fits <- fits[,`:=`(region = case_when(
                           (xc< -100 & yc< 37.5)==T~"SW",
                           (xc< -100 & yc>= 37.5)==T~"NW", 
                           (xc>= -100 & yc>= 37.5)==T~"NE", 
                          (xc>= -100 & yc< 37.5)==T~"SE"))]
fits[,`:=`(fmonth = month(month,label=T))]

q41 <- d41[,.(lst_lo = quantile(lst,0.1), 
       lst_med = median(lst), 
       lst_hi = quantile(lst,0.9)), 
  by=.(xc,yc,lc,
    month,year)]
q42 <- d42[,.(lst_lo = quantile(lst,0.1), 
       lst_med = median(lst), 
       lst_hi = quantile(lst,0.9)), 
  by=.(xc,yc,lc,
    month,year)]
q43 <- d43[,.(lst_lo = quantile(lst,0.1), 
       lst_med = median(lst), 
       lst_hi = quantile(lst,0.9)), 
  by=.(xc,yc,lc,
    month,year)]
gc(full=T)

fits <- merge(fits, rbindlist(list(q41,q42,q43)), by=c("xc","yc","lc","year","month"))

arrow::write_parquet(fits, "../data_general/proc_sif-optim/mod_fits_max_sif_monthly/fits_to_plot_topt_sifopt.parquet", compression = 'snappy')


# Plotting ----------------------------------------------------------------

cbbPalette <- c("#000000", "#E69F00", "#56B4E9", "#009E73", "#F0E442", "#0072B2", "#D55E00", "#CC79A7")
(p_topt <- fits[term=='Topt'][p.value<0.05][std.error<5] %>% 
  # .[sample(.N, 10000)] %>% 
  ggplot(data=., aes(lst_med,estimate,color=lc_class))+
  geom_segment(aes(x=lst_lo,xend=lst_hi,y=estimate,yend=estimate), 
    col='grey70',
    lwd=0.1)+
  geom_point(alpha=0.05,size=0.2,col='black')+
  # geom_point(data=fits[term=='lst'][p.value<0.05][std.error<0.02], 
  #   aes(lst_med,lst_hi), col='red')+
  geom_abline(lty=2,col='black')+
  geom_smooth(method='lm',lwd=0.5)+
  scale_color_manual(values=cbbPalette[-1])+
  labs(x=expression(paste("Land Surface Temperature (°C)")),
      y=expression(paste(T[opt]~"(°C)")),
    color='Land cover')+
  facet_grid(region~fmonth)+
  coord_equal()+
  theme_linedraw()+
  theme(panel.grid = element_blank(), 
    legend.position = 'none'))

cbbPalette <- c("#000000", "#E69F00", "#56B4E9", "#009E73", "#F0E442", "#0072B2", "#D55E00", "#CC79A7")
(p_sifopt <- fits[term=='kopt'][p.value<0.05][std.error<5] %>%
  # .[sample(.N, 10000)] %>% 
  ggplot(data=., aes(lst_med,estimate,color=lc_class))+
  geom_segment(aes(x=lst_lo,xend=lst_hi,y=estimate,yend=estimate), 
    col='grey70',
    lwd=0.1)+
  geom_point(alpha=0.1,size=0.1,col='black')+
  # geom_abline(lty=2,col='black')+
  geom_smooth(method='lm',lwd=0.5)+
  scale_color_manual(values=cbbPalette[-1])+
  # scale_color_viridis_d(option='D', begin=0.1, end=0.75)+
  labs(x=expression(paste("Land Surface Temperature (°C)")),
    y=expression(paste("SIF"[opt]~"(mW"**2,m**-2,sr**-1,nm**-1,")")),
    color='Land cover')+
  facet_grid(region~fmonth)+
  # coord_equal()+
  theme_linedraw()+
  theme(panel.grid = element_blank(),
    legend.position = 'bottom'))

(p_topt/p_sifopt)+
  # plot_layout(guides='collect')+
  plot_annotation(tag_levels = 'a',
  tag_suffix = ')',
  tag_prefix = '(')
ggsave(filename = "figures/fig_sif_topt_sif-opt_month_region.png",
  width=14,
  height=22,
  units='cm',
  dpi = 350)
        

# map ---
conus <- rnaturalearth::ne_states(
  country='United States of America',
  returnclass = 'sf')
conus <- conus %>% 
  # filter(region=='West') %>%
  filter(type=='State') %>% 
  filter(!name_en %in% c("Alaska","Hawaii"))

fits[term=='lst'][p.value<0.05][std.error<0.02][year!=2020] %>%
  ggplot(aes(xc,yc,color=estimate))+
  geom_sf(data=conus,color='black',fill=NA, inherit.aes = F)+
  # geom_tile()+
  geom_point(size=0.25,position = 'jitter')+
  # scale_fill_gradient2(limits=c(-0.1,0.1))+
  scale_color_gradient2(limits=c(-0.1,0.1))+
  coord_sf()+
  labs(x=NULL,
    y=NULL,
    color=expression(paste(beta~LST~"(°C)")))+
  facet_grid(year~fmonth,drop = T)+
  theme_linedraw()+
  theme(panel.grid = element_blank(),
    panel.background = element_rect(fill='grey80'))

# Topt range
p_topt_range <- fits[year!=2020][term%in%c('Topt')][p.value<0.05][,
  .(topt_max = max(estimate), 
    topt_min = min(estimate), 
    delta = max(estimate)-min(estimate)), 
  by=.(xc,yc,lc_class)] %>% 
    ggplot(aes(xc,yc,fill=delta))+
  geom_sf(data=conus,color='black',fill=NA, inherit.aes = F)+
  geom_tile()+
  scale_fill_viridis_c(option='H', 
    limits=c(0,20), 
    oob=scales::squish)+
  coord_sf(expand = F)+
  labs(x=NULL,
    y=NULL,
    fill=expression(paste(Range~T['opt']~"(°C)")))+
  facet_wrap(~lc_class,ncol = 1)+
  theme_linedraw()+
  theme(panel.grid = element_blank(),
    panel.background = element_rect(fill='grey80'), 
    legend.position = 'bottom'
    # legend.justification = c(1,0)
    )

# SIF opt range
p_sif_range <- fits[year!=2020][term%in%c('kopt')][p.value<0.05][,
  .(kopt_max = max(estimate), 
    kopt_min = min(estimate), 
    delta = max(estimate)-min(estimate)), 
  by=.(xc,yc,lc_class)] %>% #pull(delta) %>% quantile(., c(0.01,0.99))
    ggplot(aes(xc,yc,fill=delta))+
  geom_sf(data=conus,color='black',fill=NA, inherit.aes = F)+
  geom_tile()+
  scale_fill_viridis_c(option='H', 
    limits=c(0,2.5), 
    oob=scales::squish)+
  coord_sf(expand = F)+
  labs(x=NULL,
    y=NULL,
    fill=expression(paste("Range SIF"[opt]~"(mW"**2,m**-2,sr**-1,nm**-1,")"))
  )+
  facet_wrap(~lc_class,ncol = 1)+
  theme_linedraw()+
  theme(panel.grid = element_blank(),
    panel.background = element_rect(fill='grey80'), 
    legend.position = 'bottom'
    # legend.justification = c(1,0)
    )

(p_topt_range|p_sif_range)+
  # plot_layout(guides='collect')+
  plot_annotation(tag_levels = 'a',
  tag_suffix = ')',
  tag_prefix = '(')
ggsave(filename = "figures/fig_map_topt-range_peak-sif-range.png",
  width=22,
  height=18,
  units='cm',
  dpi = 350)


fits[year!=2020][term%in%c('lst','Topt')][p.value<0.05][,
  .(nobs = .N, 
    n_lst = sum(term=='lst'),
    n_topt = sum(term=='Topt')), by=.(xc,yc)][,
      .(frac_topt = n_topt/nobs),
      by=.(xc,yc)] %>% 
  ggplot(aes(xc,yc,fill=frac_topt))+
  geom_sf(data=conus,color='black',fill=NA, inherit.aes = F)+
  geom_tile()+
  # geom_point(size=0.25,position = 'jitter')+
  # scale_color_viridis_c()+
  scale_fill_viridis_c(option='F')+
  # scale_fill_gradient2(midpoint = 0.5)+
  # scale_color_gradient2(limits=c(-0.1,0.1))+
  coord_sf(expand = F)+
  labs(x=NULL,
    y=NULL,
    fill=expression(paste(frac(N['Topt'],N['fits'])))
    # color=expression(paste(beta~LST~"(°C)"))
    )+
  # facet_grid(year~fmonth,drop = T)+
  theme_linedraw()+
  theme(panel.grid = element_blank(),
    panel.background = element_rect(fill='grey80'), 
    legend.position = c(1,0), 
    legend.justification = c(1,0))
ggsave(filename = "figures/fig_map_frac-topt-better-than-lm-fit.png",
  width=20,
  height=12,
  units='cm',
  dpi = 350)


fits[year!=2020][term%in%c('lst')][p.value<0.05] %>% 
  ggplot(data=.,aes(x=fmonth,y=estimate))+
  geom_violin(draw_quantiles = c(0.25, 0.5, 0.75), 
    scale='count', 
    trim=F)+
  coord_cartesian(ylim=c(-0.1,0.1))+
  scale_fill_viridis_c()+
  facet_grid(region~year)
