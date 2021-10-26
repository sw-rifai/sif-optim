# Description: Plot the model fits
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
daymet <- daymet[is.na(tmax)==F]
setnames(daymet,'tmax','tamax')
setnames(daymet,c("x","y"),c("xc","yc"))
setkeyv(daymet,cols = c("xc","yc","date"))
d41 <- merge(d41,daymet,by=c('xc','yc','date'))
d42 <- merge(d42,daymet,by=c('xc','yc','date'))
d43 <- merge(d43,daymet,by=c('xc','yc','date'))


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
l1 <- rbindlist(list(l1,l2,l3))
rm(l2,l3); gc(full=T)
setkeyv(l1,cols=c("x","y","date"))
gc(full=T)
d41_1 <- d41[year==2018]
setkeyv(d41_1,cols=c("x","y","date"))
d41_1 <- l1[d41_1,roll='nearest']

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


fits %>% dcast(region+p.value~term)

merge(fits[term=='Topt'][p.value<0.05], 
  s_lai, by=c("xc","yc","year","month")) %>% 
  # filter(month==7) %>% 
  # sample_n(10000) %>%
  ggplot(data=.,aes(pdsi,estimate,color=lc_class))+
  # geom_point()+
  geom_smooth(method='lm')+
  geom_hline(aes(yintercept=mean(estimate)),col='black',lty=3)+
  labs(x='PDSI',
    y=expression(paste(T[opt]~"(°C)")), 
    color='Land Cover')+
  scale_color_viridis_d(option='D',begin=0.1, end=0.7)+
  facet_grid(fmonth~region,scales='fixed')+
  theme_linedraw()+
  theme(panel.grid = element_blank(), 
    legend.position = 'bottom')
ggsave(
  filename = "figures/prelim_Topt-pdsi_by-region-month.png",
  width=15*1.3,
  height=12*1.3,
  units='cm',
  dpi=300)


fits[term=='Topt'][p.value<0.05] %>% 
  # filter(month==7) %>% 
  # sample_n(10000) %>%
  ggplot(data=.,aes(estimate,color=lc_class,fill=lc_class))+
  geom_density()+
  geom_vline(aes(xintercept=mean(estimate)))+
  labs(
    # y=expression(paste("SIF (mW"**2,m**-2,sr**-1,nm**-1,")")),
    y=NULL,
    x=expression(paste(T[opt]~"(°C)")), 
    color='Land Cover', 
    fill='Land Cover')+
  scale_fill_viridis_d(option='D',begin=0.1, end=0.7,alpha=0.5)+
  scale_color_viridis_d(option='D',begin=0.1, end=0.7)+
  facet_grid(region~fmonth,scales='fixed')+
  theme_linedraw()+
  theme(panel.grid = element_blank(), 
    legend.position = 'bottom')
ggsave(
  filename = "figures/prelim_Topt-density_by-region-month.png",
  width=20,
  height=12.5,
  units='cm',
  dpi=300)

fits[term=='lst'][p.value<0.05] %>% 
  ggplot(data=.,aes(estimate,color=lc_class,fill=lc_class))+
  geom_density(bw=0.01)+
  geom_vline(aes(xintercept=mean(estimate)))+
  labs(
    # y=expression(paste("SIF (mW"**2,m**-2,sr**-1,nm**-1,")")),
    x=expression(paste(Delta~"SIF"~"/"~"°C")),
    color='Land Cover', 
    fill='Land Cover')+
  scale_fill_viridis_d(option='D',begin=0.1, end=0.7,alpha=0.5)+
  scale_color_viridis_d(option='D',begin=0.1, end=0.7)+
  facet_grid(region~fmonth,scales='free')+
  theme_linedraw()+
  theme(panel.grid = element_blank(), 
    legend.position = 'bottom')
ggsave(
  filename = "figures/prelim_BetaLST-density_by-region-month.png",
  width=20,
  height=12.5,
  units='cm',
  dpi=300)



fits[term=='Topt'][p.value<0.05] %>% 
  ggplot(data=.,aes(lst,estimate,
    color=lc_class))+
  geom_smooth(method='lm')+
  geom_point(
    size=0.5,alpha=0.25)+
  # scale_fill_viridis_c(option='D',begin=0.2, end=0.6)+
  # scale_color_viridis_c(option='H')+
  geom_abline(color='black',lty=2)+
  labs(x=expression(paste(Mean~LST[month]~"(°C)")),
    y=expression(paste(T[opt]~"(°C)")),
    color='Land Cover',
    fill='Land Cover'
    )+
  scale_color_viridis_d(option='D',begin=0.2, end=0.6)+
  coord_equal()+
  facet_grid(region~fmonth,scales = 'fixed')+
  theme_linedraw()+
  theme(panel.grid = element_blank(), 
    legend.position = 'bottom')
ggsave(
  filename = "figures/prelim_Topt-LSTmonmean_by-region-month.png",
  width=20,
  height=18,
  units='cm',
  dpi=300)


fits[term=='kopt'][p.value<0.05] %>% 
  ggplot(data=.,aes(lst,estimate,color=lc_class))+
  geom_point(size=0.5,alpha=0.25,color='grey60')+
  # geom_abline(color='black',lty=2)+
  geom_smooth(method='lm')+
  labs(x=expression(paste(Mean~LST[month]~"(°C)")),
    y=expression(paste(K[opt]~"(mW"**2~m**-2~sr**-1~nm**-1,")")),
    color='Land Cover',
    fill='Land Cover')+
  scale_fill_viridis_d(option='D',begin=0.2, end=0.6)+
  scale_color_viridis_d(option='D',begin=0.2, end=0.6)+
  facet_grid(region~fmonth)+
  theme_linedraw()+
  theme(panel.grid = element_blank(), 
    legend.position = 'bottom')
ggsave(
  filename = "figures/prelim_kopt-LSTmonmean_by-region-month.png",
  width=20,
  height=18,
  units='cm',
  dpi=300)



fits[term=='kopt'][p.value<0.05] %>% 
  ggplot(data=.,aes(pdsi,lst,color=lc_class))+
  geom_point(size=0.5,alpha=0.25,color='grey60')+
  # geom_abline(color='black',lty=2)+
  geom_smooth()+
  # labs(x=expression(paste(Mean~LST[month]~"(°C)")),
  #   y=expression(paste(K[opt]~"(mW"**2~m**-2~sr**-1~nm**-1,")")),
  #   color='Land Cover',
  #   fill='Land Cover')+
  scale_fill_viridis_d(option='D',begin=0.2, end=0.6)+
  scale_color_viridis_d(option='D',begin=0.2, end=0.6)+
  facet_grid(region~fmonth)+
  theme_linedraw()+
  theme(panel.grid = element_blank(), 
    legend.position = 'bottom')



fits[term=='kopt'][p.value<0.05] %>% 
  # filter(month==7) %>% 
  # sample_n(10000) %>%
  ggplot(data=.,aes(pdsi,estimate,color=lc_class))+
  # geom_point()+
  geom_smooth(method='lm')+
  geom_hline(aes(yintercept=mean(estimate)),col='black',lty=3)+
  labs(x='PDSI',
    y=expression(paste(K[opt]~"()")), 
    color='Land Cover')+
  scale_color_viridis_d(option='D',begin=0.1, end=0.7)+
  facet_grid(fmonth~region)+
  theme_linedraw()+
  theme(panel.grid = element_blank())



fits[term=='kopt'][p.value<0.05] %>% 
  ggplot(data=.,aes(pdsi,estimate,color=lc_class))+
  geom_point(size=0.5,alpha=0.25,color='grey60')+
  # geom_abline(color='black',lty=2)+
  geom_smooth(method='lm')+
  labs(
    x='PDSI',
    # x=expression(paste(Mean~LST[month]~"(°C)")),
    y=expression(paste(K[opt]~"(mW"**2~m**-2~sr**-1~nm**-1,")")),
    color='Land Cover',
    fill='Land Cover')+
  scale_fill_viridis_d(option='D',begin=0.2, end=0.6)+
  scale_color_viridis_d(option='D',begin=0.2, end=0.6)+
  facet_grid(region~fmonth)+
  theme_linedraw()+
  theme(panel.grid = element_blank(), 
    legend.position = 'bottom')

# Compare EC & SIF Topt -------------------------------------------------------
of_fits <- arrow::read_parquet("../data_general/proc_sif-optim/mod_fits/fluxnet2015_oneflux/fluxnet2015-oneflux_topt-fits_2021-09-27.parquet")
of_sites <- fread("data/fluxnet15_US_sites.csv") %>% 
  rename(x=longitude,y=latitude,site=SITE_ID) %>% 
  select(site,x,y) %>% 
  mutate(xc=round(x/4)*4, 
    yc=round(y/4)*4)
of_sites
of_fits[,`:=`(site=str_remove(site,"FLX_"))]
of_fits$site %in% of_sites$site %>% table
of_fits <- of_fits[,`:=`(region = case_when(
                           (xc< -100 & yc< 37.5)==T~"SW",
                           (xc< -100 & yc>= 37.5)==T~"NW", 
                           (xc>= -100 & yc>= 37.5)==T~"NE", 
                          (xc>= -100 & yc< 37.5)==T~"SE"))]
of_fits[,`:=`(fmonth = month(month,label=T))]


of_fits <- merge(of_fits, of_sites,by='site')

merge(of_fits[term=='Topt'][p.value<0.05][std.error<5], 
  fits[term=='Topt'][p.value<0.05][std.error<5], 
  by=c("region","month"), 
  suffix=c("_ec","_sif"), allow.cartesian = T) %>% 
  .[,.(region, month, estimate_ec,estimate_sif)] %>% 
  melt(., id.vars=c("region","month")) %>% 
  ggplot(data=.,aes(value,fill=variable))+
  # geom_histogram(bins=50 ,aes(width,density))+
  geom_density(bw=1, alpha=0.5)+
  facet_grid(month~region,scales = 'free')

of_fits %>% select(xc,yc,site) %>% 
  unique() %>% 
  ggplot(data=.,aes(xc,yc,label=site))+
  geom_label()
of_fits[site=="US-Me2"] %>% select(xc,yc) %>% summary

fits[term=='Topt'][p.value<0.05][std.error<5][region=='NW'][fmonth=='Aug'] %>% 
  .[near(xc,-120)] %>% 
  .[near(yc,44)] %>% 
  .$estimate %>% summary


library(mgcv)
b1 <- bam(estimate~
    scale(lst)+
    scale(pdsi)+
    lc_f+
    te(xc,yc,by=fmonth), 
  data=fits[term=='Topt'][p.value<0.05][std.error<5][,lc_f := factor(lc)])
summary(b1)
plot(b1,scheme = 2)


b2 <- bam(lst~
    s(pdsi)+
    lc_f+
    te(xc,yc,by=fmonth), 
  data=fits[term=='Topt'][p.value<0.05][std.error<5][,lc_f := factor(lc)])
summary(b2)
plot(b2)

fits %>% select(lst,pdsi) %>% cor

b3 <- bam(estimate~
    lst+
    lc_f+
    factor(region)+
    te(xc,yc,by=fmonth), 
  data=fits[term=='Topt'][p.value<0.05][std.error<5][,lc_f := factor(lc)])
summary(b3)

bam(estimate~tmean+
    fmonth*site+
    factor(region), 
  data=of_fits[term=='Topt'][p.value<0.05][std.error<5] %>% 
    mutate(site = factor(site))) %>% 
  summary

library(lme4)
lmer(estimate~tmean+
    (tmean|fmonth)+
    (tmean|region)+
    (tmean|site),
  data=of_fits[term=='Topt'][p.value<0.05][std.error<5] %>% 
    mutate(site = factor(site))) %>% 
  summary

lmer(estimate~lst+
    (lst|fmonth)+
    (lst|region)+
    ,
  data=fits[term=='Topt'][p.value<0.05][std.error<5]) %>% 
  summary
