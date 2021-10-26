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
 
# options -----------------------------------------------------------------
lc_class <- 41

# prep dat ----------------------------------------------------------------
dat <- arrow::read_parquet(paste0("../data_general/proc_sif-optim/parquet-by-lc/sif_lst_pdsi_lc",lc_class,".parquet")) %>% .[,fmonth := month(date,label = T)]

sdat <- dat[,`:=`(clst = round(lst,1))][
  ,.(sif = max(sif), 
    pdsi = mean(pdsi,na.rm=T)),
  by=.(id,xc,yc,clst,dc,year,month)]

fname <- list.files("../data_general/proc_sif-optim/mod_fits_max_sif_monthly/",
  pattern=paste0("lc-",lc_class,"-max-sif-monthly-fits_\\d{4}"), 
  full.names = T) 

fits <- arrow::read_parquet(fname)
setDT(fits)
fits[,.(val=sum(term=='Topt')),by=dc]

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

# Get name of LC ------------------------------------------------------
lc_name <- case_when(lc_class==41~"Decid. Forest", 
                   lc_class==42~"Evergreen Forest",
                   lc_class==43~"Mixed Forest",
                   lc_class==90~"Woody Wetlands") 


## Fig: Maps of Topt and Beta LST --------------------------------------
conus <- rnaturalearth::ne_states(
  country='United States of America',
  returnclass = 'sf')
conus <- conus %>% 
  # filter(region=='West') %>%
  filter(type=='State') %>% 
  filter(!name_en %in% c("Alaska","Hawaii"))


vec_lims <- fits %>% filter(term=='Topt') %>% 
  filter(p.value < 0.05) %>% pull(estimate) %>% quantile(.,c(0.025,0.975))
fits %>% 
  filter(term=='Topt') %>% 
  # filter(dc=='norm') %>% 
  filter(p.value < 0.05) %>% #pull(estimate) %>% quantile(.,c(0.05,0.95))
  ggplot(data=.,aes(xc,yc,fill=estimate))+
  geom_sf(data=conus,inherit.aes = F)+
  geom_tile()+
  scale_fill_viridis_c(option='B', 
    limits=vec_lims, 
    oob=scales::squish)+
  coord_sf(expand = F)+
  labs(x=NULL,y=NULL,
    title=paste0(lc_name),
    fill=expression(T[opt]~"(°C)"))+
  facet_wrap(~dc,ncol=1)+
  theme_linedraw()+
  theme()
out_fig_name <- paste0("figures/prelim_lc-",lc_class,"_map-Topt_by-pdsi.png")
ggsave(
  filename = out_fig_name,
  width=15,
  height=20,
  units='cm',
  dpi=300)
# END FIG **************************************************************

# scratch ***
# 
# 

mnobs <- sdat[,.(nobs = .N), by=.(id,xc,yc,year,month)]
mnobs$nobs %>% hist
mnobs[between(nobs,10,20)]
merge(mnobs[between(nobs,200,210)][sample(.N,10)],sdat,all.x=T,all.y=F) %>% 
  ggplot(data=.,aes(clst,sif))+
  geom_point()+
  geom_smooth()+
  facet_wrap(~id)
paf_dt(merge(mnobs[between(nobs,50,60)][1,],sdat,all.x=T,all.y=F) %>% 
    rename(lst=clst))

paf_dt(sdat[id==7501&year==2020&month==6]%>% 
    rename(lst=clst))
paf_dt(sdat[id==1&year==2018&month==7] %>% 
    rename(lst=clst))
paf_dt(sdat[id==1&year==2018&month==8] %>% 
    rename(lst=clst))
paf_dt(sdat[id==1&year==2018&month==9] %>% 
    rename(lst=clst))

j1 <- bam(estimate~
    dc+
    s(mappet,k=5)+
    s(p_jja,k=5)+
    s(pdsi_jja,k=5)+
    s(pet_jja,k=5)+
    # s(vpd_jja,k=5)+
    # s(srad_jja,k=5)+
    # s(matmin_jja,k=5)+
    s(map,k=5)+
    s(matmax_jja,k=5)+
    s(elevation,k=5)+
    # te(map,matmax_jja,elevation,k=5)+
    te(xc,yc),
    # s(matmax_jja,dc,bs='fs',k=5)+
    # s(map,dc,bs='fs',k=5)+
    # s(elevation,dc,bs='fs',k=5), 
  data=fits %>% 
  filter(term=='Topt') %>% 
  filter(p.value < 0.05) %>% 
  filter(estimate < 45) %>% 
    mutate(dc = factor(dc)), 
  select=T) 
summary(j1)
plot(j1,scheme=2)

j1 <- bam(sif~
    # dc+
    # pdsi_jja*mappet+
    # dc*map+
    # dc*pet_jja+
    # s(p_jja,k=5)+
    # s(pdsi_jja,k=5)+
    # s(pet_jja,k=5)+
    # s(map,k=5)+
    # s(matmax_jja,k=5)+
    # s(elevation,k=5)+
    # dc+
    te(xc,yc,by=clst),
  data=sdat[dc=='wet'][sample(.N,1e6)],
  # data=fits %>% 
  # filter(term=='Topt') %>% 
  # filter(p.value < 0.05) %>% 
  # filter(estimate < 45) %>% 
  #   mutate(dc = factor(dc)), 
  discrete=T,
  select=T) 
summary(j1)
library(mgcViz)
palette <- pals::brewer.rdylbu(5)
mgcViz::getViz(j1) %>% sm(.,1) %>% 
  plot()+l_fitRaster()+
  l_fitContour()+
  scale_fill_gradient2(low = palette[1],mid=palette[3],high=palette[5], 
    limits=c(-0.05,0.05),
    oob=scales::squish)+
mgcViz::getViz(j1) %>% plot(allTerms=T) %>% print(pages=1)

fits %>% 
  filter(term=='Topt') %>% 
  filter(p.value < 0.05) %>% 
  filter(estimate < 45) %>% 
  filter(mappet<2.5) %>% 
  ggplot(data=.,aes(mappet,estimate,color=dc,group=dc))+
  geom_point(alpha=0.25,size=0.25)+
  geom_smooth(method='lm')+
  # geom_smooth(formula=y~s(x,bs='cs',k=5))+
  labs(x=expression(paste(Seasonal~Mean~Tmax[JJA]~"(°C)")), 
        # y=expression(paste("SIF (mW"**2,m**-2,sr**-1,nm**-1,")")),
        y=expression(SIF~~T[opt]~("°C")), 
    title=lc_name)+
  # facet_wrap(~dc)+
    theme_linedraw()+
  theme(strip.background = element_rect(fill='white'),
    strip.text = element_text(color='black'), 
    panel.background = element_blank());

## Fig: Topt by Tmax and MAP ----------------------------------
p_top <- fits %>% 
  filter(term=='Topt') %>% 
  filter(p.value < 0.05) %>% 
  filter(estimate < 45) %>% 
  ggplot(data=.,aes(matmax_jja*0.1,estimate))+
  geom_point(alpha=0.25,size=0.25)+
  geom_smooth(method='lm',color="#CF0000")+
  # geom_smooth(formula=y~s(x,bs='cs',k=5))+
  labs(x=expression(paste(Seasonal~Mean~Tmax[JJA]~"(°C)")), 
        # y=expression(paste("SIF (mW"**2,m**-2,sr**-1,nm**-1,")")),
        y=expression(SIF~~T[opt]~("°C")), 
    title=lc_name)+
  facet_wrap(~dc)+
    theme_linedraw()+
  theme(strip.background = element_rect(fill='white'),
    strip.text = element_text(color='black'), 
    panel.background = element_blank()); p_top
p_bot <- fits %>% 
  filter(term=='Topt') %>% 
  filter(p.value < 0.05) %>% 
  filter(estimate < 45) %>% 
  ggplot(data=.,aes(map,estimate))+
  geom_point(alpha=0.25,size=0.25)+
  geom_smooth(method='glm',
    method.args=list(family=Gamma(link='log')),
    color="#0390fc")+
  # geom_smooth(formula=y~s(x,bs='cs',k=5))+
  labs(x=expression(paste("Mean Annual Precip. (",mm~yr**-1,")")), 
        # y=expression(paste("SIF (mW"**2,m**-2,sr**-1,nm**-1,")")),
        y=expression(SIF~~T[opt]~("°C")), 
    # title='Deciduous Forest'
    )+
  facet_wrap(~dc)+
    theme_linedraw()+
  theme(strip.background = element_rect(fill='white'),
    strip.text = element_text(color='black'), 
    panel.background = element_blank()); p_bot
fig_name <- paste0("figures/prelim_",str_remove(lc_name," "),"-forest_sif-Topt-tmax-map_by-pdsi.png")

ggsave(plot=p_top/p_bot, 
  filename = fig_name,
  width=20,
  height=15,
  units='cm',
  dpi=300)
# END FIG ********************************


## Fig: ---------------------------------------------------------------
palette <- pals::brewer.rdylbu(5)
fits %>% 
  filter(term=='Topt') %>% 
  # filter(dc=='norm') %>% 
  filter(p.value < 0.05) %>% 
  pivot_wider(id_cols=c(xc,yc), names_from=c(dc), 
    values_from=estimate) %>% 
  mutate(delta = norm-wet) %>% 
  filter(is.na(delta)==F) %>% 
  ggplot(data=.,aes(xc,yc,color=delta))+
  geom_sf(data=conus,inherit.aes = F)+
  geom_point()+
  scale_color_gradient2(low = palette[1],mid=palette[3],high=palette[5], 
    limits=c(-5,5), 
    oob=scales::squish)+
  coord_sf(expand = F)+
  labs(x=NULL,y=NULL,
    color=expression(Delta~T[opt]~"(°C)"))+
  theme_linedraw()+
  theme()
# END FIG **************************************************************

fits %>% 
  filter(term=='Topt') %>% 
  # filter(dc=='norm') %>% 
  filter(p.value < 0.05) %>% 
  pivot_wider(id_cols=c(xc,yc,matmax_jja), names_from=c(dc), 
    values_from=estimate) %>% 
  mutate(delta = norm-wet) %>% 
  filter(is.na(delta)==F) %>% 
  ggplot(data=.,aes(matmax_jja*0.1,delta))+
  geom_point()

fits[term=='Topt'][p.value<0.05][dc=='drought'] %>% 
  .[,c('estimate',"map", "matmax_jja", "matmin_jja", "pdsi_jja", "pet_jja", 
"srad_jja", "vpd_jja")] %>% 
  cor() %>% 
  corrplot::corrplot(method='number')

fits[term=='Topt'][p.value<0.05][dc=='norm'] %>% 
  .[,c('estimate',"map", "matmax_jja", "matmin_jja", "pdsi_jja", "pet_jja", 
"srad_jja", "vpd_jja")] %>% 
  cor() %>% 
  corrplot::corrplot(method='number')

fits[term=='Topt'][p.value<0.05][dc=='wet'] %>% 
  .[,c('estimate',"map", "matmax_jja", "matmin_jja", "pdsi_jja", "pet_jja", 
"srad_jja", "vpd_jja")] %>% 
  cor() %>% 
  corrplot::corrplot(method='number')

fits[term%in%c("Hd","Ha")] %>% 
  # .[p.value<0.05] %>% 
  pivot_wider(.,id_cols=c(id,dc),names_from=term,values_from=c(estimate,p.value)) %>% 
  ggplot(data=.,aes(estimate_Hd,estimate_Ha,color=p.value_Hd))+
  geom_point()+
  coord_cartesian(xlim=c(0,1),
    ylim=c(0,1))+
  scale_color_viridis_c()

sdat[between(clst,0,45)] %>% 
  ggplot(data=.,aes(pdsi,sif,color=cut_interval(clst,5)))+
  # geom_point()+
  geom_smooth(method='lm')+
  scale_color_viridis_d(option='B',direction = 1)+
  facet_grid(cut(yc,c(25,40,60))~cut(xc,c(-127,-99,65)))


sdat[between(clst,0,45)] %>% 
  ggplot(data=.,aes(clst,sif,color=cut_interval(pdsi,5)))+
  # geom_point()+
  geom_smooth()+
  scale_color_viridis_d(option='B',direction = 1)+
  facet_grid(cut(yc,c(25,40,60))~cut(xc,c(-127,-99,65)))
    