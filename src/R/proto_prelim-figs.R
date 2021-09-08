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

pacman::p_load(tictoc, tidyverse,  
  data.table,lubridate,
  stars, mgcv, mgcViz, 
  patchwork)

# LC-41 Deciduous forests ---------------------------------------------------
## d41 as in data for fitting LC-41
d41 <- arrow::read_parquet(paste0("../data_general/proc_sif-optim/parquet-by-lc/sif_lst_pdsi_lc",41,".parquet"))
d41[,fmonth := month(date,label = T)]

s41 <- d41[,`:=`(clst = round(lst,1))][
  ,.(sif = max(sif,na.rm=T)),
  by=.(id,xc,yc,clst,dc)]

f41 <- arrow::read_parquet(("../data_general/proc_sif-optim/mod_fits/lc-41-fits_2021-09-04.parquet"))
setDT(f41)
f41[,.(val=sum(term=='Topt')),by=dc]

tc <- stars::read_stars(
  list.files("../data_general/proc_sif-optim/conus_terraclim/",'tif',
    full.names = T))
names(tc) <- str_remove(names(tc),".tif") %>% str_remove(., "terraclimate_")

ref_x <- seq(from=range(f41$xc)[1],to=range(f41$xc)[2],by=0.25)
ref_y <- seq(from=range(f41$yc)[1],to=range(f41$yc)[2],by=0.25)
ref <- st_as_stars(expand_grid(x=ref_x,y=ref_y),crs=st_crs(4326))
tc <- st_warp(tc,ref,use_gdal = F)
tc <- tc %>% as.data.table()
tc <- tc[is.na(map)==F]

f41 <- merge(f41,tc,by.x=c("xc","yc"),by.y=c("x","y"))

nobs41 <- d41[year<2020][,.(nobs=.N),by=.(id,year,month)]


## Fig: Maps of Topt and Beta LST --------------------------------------
conus <- rnaturalearth::ne_states(
  country='United States of America',
  returnclass = 'sf')
conus <- conus %>% 
  # filter(region=='West') %>%
  filter(type=='State') %>% 
  filter(!name_en %in% c("Alaska","Hawaii"))

f41 %>% 
  filter(term=='Topt') %>% 
  # filter(dc=='norm') %>% 
  filter(p.value < 0.05) %>% 
  ggplot(data=.,aes(xc,yc,fill=estimate))+
  geom_sf(data=conus,inherit.aes = F)+
  geom_tile()+
  scale_fill_viridis_c(option='B')+
  coord_sf(expand = F)+
  labs(x=NULL,y=NULL,
    fill=expression(T[opt]~"(°C)"))+
  facet_wrap(~dc,ncol=1)+
  theme_linedraw()+
  theme()
ggsave(
  filename = "figures/prelim_decid-forest_map-Topt_by-pdsi.png",
  width=15,
  height=20,
  units='cm',
  dpi=300)
# END FIG **************************************************************

## Fig: Maps of Topt and Beta LST --------------------------------------
conus <- rnaturalearth::ne_states(
  country='United States of America',
  returnclass = 'sf')
conus <- conus %>% 
  # filter(region=='West') %>%
  filter(type=='State') %>% 
  filter(!name_en %in% c("Alaska","Hawaii"))

palette <- pals::brewer.rdylbu(5)
f41 %>% 
  filter(term=='lst') %>% 
  # filter(dc=='norm') %>% 
  filter(p.value < 0.05) %>% 
  ggplot(data=.,aes(xc,yc,color=estimate))+
  geom_sf(data=conus,inherit.aes = F)+
  geom_point(size=0.5)+
  scale_color_gradient2(low = palette[1],mid=palette[3],high=palette[5], 
    limits=c(-0.1,0.1), 
    oob=scales::squish)+
  coord_sf(expand = F)+
  labs(x=NULL,y=NULL,
    color=expression(beta[LST]~"(°C)"))+
  facet_wrap(~dc,ncol=1)+
  theme_linedraw()+
  theme()
ggsave(
  filename = "figures/prelim_decid-forest_map-BetaLST_by-pdsi.png",
  width=15,
  height=20,
  units='cm',
  dpi=300)
# END FIG **************************************************************

## Fig: compare T peak by filtering ------------------------------------
vec_ids <- sample(unique(s41$id),40)
p1 <- d41[id%in%vec_ids] %>% 
  ggplot(data=.,aes(clst,sif,group=id,color=factor(id)))+
  # geom_point()+
  geom_smooth(method='gam', 
    formula=y~s(x,bs='cs',k=5), 
    se=F, lwd=0.5)+
  scale_color_viridis_d(option='A',direction = -1,end=0.9)+
  labs(title='Decid. Forest: No Filtering', 
    x = expression(paste("LST (°C)")),
    y=expression(paste("SIF (mW"**2,m**-2,sr**-1,nm**-1,")")))+
  facet_wrap(~dc,scales='free',ncol=1)+
  theme_linedraw()+
  theme(legend.position = 'none', 
    panel.grid = element_blank())

p2 <- s41[id%in%vec_ids] %>% 
   # na.omit() %>% pull(id) %>% unique
  ggplot(data=.,aes(clst,sif,group=id,color=factor(id)))+
  # geom_point()+
  geom_smooth(method='gam', 
    formula=y~s(x,bs='cs',k=5), 
    se=F, 
    lwd=0.5)+
  scale_color_viridis_d(option='A',direction = -1,end=0.9)+
  labs(title='Filtered: Max SIF per 0.1 °C', 
    x = expression(paste("LST (°C)")),
    y=expression(paste("SIF (mW"**2,m**-2,sr**-1,nm**-1,")")))+
  facet_wrap(~dc,scales='free',ncol=1)+
  theme_linedraw()+
  theme(legend.position = 'none', 
    panel.grid = element_blank())

ggsave(plot=p1|p2,
  filename = "figures/prelim_decid-forest_compare-data-filtering_gam-lst.png",
  width=15,
  height=15,
  units='cm',
  dpi=300)
# END FIG *****************************************************

## Fig: Hysteresis of SIF -------------------------------------
library(nls.multstart)

f41[id==1301]
d41[id%in%f41[term=='Topt'][p.value<0.05]$id][
  fid%in%nobs41[nobs>500]$id][yc==max(yc)][
  is.na(sif)==F][is.na(lst)==F][dc=='norm'] %>%
  ggplot(data=.,aes(lst,sif))+
  geom_point()+
  geom_smooth(method='nls', 
    formula=y~kopt * ((Hd * (2.718282^((Ha*(x-Topt))/(x*0.008314*Topt)))) / 
              (Hd - (Ha*(1-(2.718282^((Hd*(x-Topt))/(x*0.008314*Topt))))))), 
    se=F,
    method.args=list(
         start = c(kopt = 1.6, Hd = 2.57, Ha = 0.99, Topt = 34.4),
         # start = c(kopt = 4, Hd = 200, Ha = 300, Topt = 35),
         # supp_errors = 'Y',
         na.action = na.omit,
         #convergence_count = 500,
         algorithm='port',
         lower = c(kopt = 0.1, Hd = 0, Ha = 0, Topt = 0),
         upper = c(kopt = 20, Hd = 1000,Ha=1000,Topt=50))
  )

d41[id%in%sample(f41[term=='Topt'][p.value<0.05]$id,5)][
  fid%in%nobs41[nobs>500]$id][yc==max(yc)][
  is.na(sif)==F][is.na(lst)==F][dc=='norm'] %>%
  .[,.(sif=max(sif)),by=.(lst,id)] %>% 
  ggplot(data=.,aes(lst,sif))+
  geom_point()+
    geom_smooth(method='nls', 
    formula=y~kopt * ((Hd * (2.718282^((Ha*(x-Topt))/(x*0.008314*Topt)))) / 
              (Hd - (Ha*(1-(2.718282^((Hd*(x-Topt))/(x*0.008314*Topt))))))), 
    se=F,
    method.args=list(
         start = c(kopt = 1.6, Hd = 2.57, Ha = 0.99, Topt = 34.4),
         # start = c(kopt = 4, Hd = 200, Ha = 300, Topt = 35),
         # supp_errors = 'Y',
         na.action = na.omit,
         #convergence_count = 500,
         algorithm='port',
         lower = c(kopt = 0.1, Hd = 0, Ha = 0, Topt = 0),
         upper = c(kopt = 5, Hd = 200,Ha=100,Topt=50))
  )+
  facet_wrap(~id)
  # geom_smooth(se=F, 
  #   method='gam',
  #   formula=y~s(x,bs='cs',k=4))+
  # facet_wrap(~year)


## Fig: Topt by Tmax and MAP ----------------------------------
library(patchwork)
p_top <- f41 %>% 
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
    title='Deciduous Forest')+
  facet_wrap(~dc)+
    theme_linedraw()+
  theme(strip.background = element_rect(fill='white'),
    strip.text = element_text(color='black'), 
    panel.background = element_blank()); p_top
p_bot <- f41 %>% 
  filter(term=='Topt') %>% 
  filter(p.value < 0.05) %>% 
  filter(estimate < 45) %>% 
  ggplot(data=.,aes(map,estimate))+
  geom_point(alpha=0.25,size=0.25)+
  geom_smooth(method='lm',
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
ggsave(plot=p_top/p_bot, 
  filename = "figures/prelim_decid-forest_sif-Topt-tmax-map_by-pdsi.png",
  width=20,
  height=15,
  units='cm',
  dpi=300)
# END FIG ********************************


f41 %>% 
  filter(term=='Topt') %>% 
  filter(p.value < 0.05) %>% 
  filter(estimate < 45) %>% 
  ggplot(data=.,aes(map,estimate))+
  geom_point()+
  geom_smooth(method='lm',color='red')+
  # geom_smooth(formula=y~s(x,bs='cs',k=5))+
  facet_wrap(~dc)

f41 %>% 
  filter(term=='Topt') %>% 
  filter(p.value < 0.05) %>% 
  ggplot(data=.,aes(x=dc, estimate))+
  geom_boxplot(outlier.colour = NA)
f41 %>% 
  filter(term=='lst') %>% 
  filter(p.value < 0.05) %>% 
  ggplot(data=.,aes(x=dc, estimate))+
  geom_boxplot(outlier.colour = NA)+
  coord_cartesian(ylim=c(-0.1,0.1))
f41[term=='rsq']$estimate %>% hist

b41 <- bam(sif~s(lst,f_pdsi,bs='fs')+te(x,y)+fmonth, 
  data=d41[sample(.N,1e5)][,f_pdsi := cut_interval(pdsi,5)], 
  select=T,
  discrete=T)
summary(b41)
plot(b41,scheme = 2,all.terms = T)
mgcViz::getViz(b41) %>% plot

b41_2 <- bam(sif~te(x,y,by=lst)+fmonth, 
  data=d41[sample(.N,1e5)][,f_pdsi := cut_interval(pdsi,5)], 
  select=T,
  discrete=T)
summary(b41_2)
plot(b41_2,scheme = 2,all.terms = T)

b41_3 <- bam(sif~
    scale(lst)*scale(pdsi)+
    te(x,y,by=fmonth), 
  data=d41[sample(.N,1e5)][,f_pdsi := cut_interval(pdsi,5)][
    sif<=4
  ], 
  # family=Gamma(link='log'),
  select=T,
  discrete=T)
summary(b41_3)
plot(b41_3,scheme = 2,all.terms = T)
mgcViz::getViz(b41_3) %>% plot

b41_4 <- bam(sif~
    te(x,y,lst,pdsi)+fmonth, 
  data=d41[sample(.N,1e6)][,f_pdsi := cut_interval(pdsi,5)][
    sif<=4
  ], 
  # family=Gamma(link='log'),
  select=T,
  discrete=T)
summary(b41_4)

v_4 <- getViz(b41_4)
pl <- plotSlice(sm(v_4,1),fix=list(pdsi=c(-5,0,5), 
                             lst=c(25,30,35)))
pl+l_fitRaster()+l_fitContour()+
  scale_fill_viridis_c(option='D',limits=c(0,4),oob=scales::squish)+
  coord_equal()

nobs41 <- d41[,.(nobs=.N),by=id]
nobs41[nobs>1000]$id[1]

## Fig: SIF~LST by PDSI --------------
b41_5 <- bam(sif~
    te(lst,pdsi, bs='cs',k=5)+
    # s(pdsi,bs='cs',k=5)+
    # s(pdsi,bs='cs',k=5)+
    te(x,y)+
    fmonth
    # s(fid,bs='re')
    ,
  select=T,
  discrete=T, 
  data=d41[id %in% sample(nobs41[nobs>200]$id,3000)][
    , fid := factor(id)])
summary(b41_5)
getViz(b41_5) %>% plot(., allTerms=T)
plot(b41_5,select = 1,scheme = 2)

d41[id %in% sample(nobs41[nobs>1000]$id,1000)]$lst %>% quantile(.,c(0.05,0.95))

library(gratia); library(colorspace)
eval_smooth(model=b41_5,
  get_smooth(b41_5,"te(lst,pdsi)"), 
  data=expand_grid(lst=seq(19,35,length.out=100),
    pdsi=c(-5,-2.5,0,2.5,5))) %>% 
  unnest('data') %>% 
  ggplot(data=.,aes(lst,est,color=factor(pdsi)))+
  geom_line()+
  scale_color_discrete_divergingx(
    palette = 'RdYlBu',
    # rev = T,
    # l1 = 70,
    l2=75,
    # h1 = 255,
    # h2=12,
    # c1 = 50,p1 = 1,p2=1.3
    )+
  labs(x=expression(paste(Land~Skin~Temperature~("°C"))), 
    y=expression(paste("SIF (mW"**2,m**-2,sr**-1,nm**-1,")")), 
    color="PDSI",
    title="Deciduous Forest (May-Sept.)")+
  theme_linedraw()+
  theme(panel.grid = element_blank())
ggsave("figures/prelim_decid-forest_gam-smooth_sif-lst-pdsi.png",
  width=15,
  height=12,
  units='cm',
  dpi=300)

## Fig: LST~PDSI by month --------------
d41[id %in% sample(nobs41[nobs>200]$id,3000)][sample(.N,2e6)] %>% 
  ggplot(data=.,aes(pdsi,lst,color=fmonth))+
  geom_smooth(#color='red', 
    method='bam',
    formula=y~s(x,bs='cs',k=5),
    method.args=list(discrete=T,
      select=T), 
    fullrange=T)+
    labs(y=expression(paste(Land~Skin~Temperature~("°C"))), 
    # y=expression(paste("SIF (mW"**2,m**-2,sr**-1,nm**-1,")")), 
    x="PDSI",
    color='month',
    title="Deciduous Forest (May-Sept.)")+
  scale_color_viridis_d(option='H')+
  scale_x_continuous(
    breaks=scales::pretty_breaks(n = 5),
    limits=c(-5.1,5.1),
    expand=c(0,0))+
  theme_linedraw()+
  theme(panel.grid = element_blank())
ggsave("figures/prelim_decid-forest_gam-smooth_lst-pdsi_by-month.png",
  width=15,
  height=12,
  units='cm',
  dpi=300)

d41$pdsi %>% hist(14)



# Evergreen forests LC-42 ---------------------------------------------------
## f42 as in fits of LC-42
f42 <- arrow::read_parquet(("../data_general/proc_sif-optim/mod_fits/lc-42-fits_2021-09-05.parquet")) %>% setDT(.)



f42 %>% 
  filter(term=='Topt') %>% 
  filter(p.value < 0.05) %>% #pull(estimate) %>% hist
  ggplot(data=.,aes(x=dc, estimate))+
  geom_boxplot(outlier.colour = NA)

f42 %>% 
  filter(term=='lst') %>% 
  filter(p.value < 0.05) %>% 
  ggplot(data=.,aes(x=dc, estimate))+
  geom_boxplot(outlier.colour = NA)+
  coord_cartesian(ylim=c(-0.1,0.1))

f42 %>% 
  filter(term=='Topt') %>% 
  filter(p.value < 0.05) %>% #pull(estimate) %>% hist
  ggplot(data=.,aes(xc,yc,fill=estimate))+
  geom_tile()+
  scale_fill_viridis_c(option='B')+
  coord_sf()+
  labs(x=NULL,y=NULL,fill='Topt')+
  facet_wrap(~dc,ncol=1)+
  theme_linedraw()+
  theme(panel.grid = element_blank())


f42 %>% 
  filter(term=='Topt') %>% 
  filter(p.value < 0.05) %>% #pull(estimate) %>% hist
  ggplot(data=.,aes(estimate,fill=dc))+
  geom_histogram(bins=50)

f42 %>% 
  filter(term=='lst') %>% 
  filter(p.value < 0.05) %>% #pull(estimate) %>% hist
  ggplot(data=.,aes(xc,yc,fill=estimate))+
  geom_tile()+
  scale_fill_gradient2(limits=c(-0.1,0.1),oob=scales::squish)+
  coord_sf()+
  theme_dark()+
  facet_wrap(~dc,ncol=1)

f42 %>% 
  filter(term=='lst') %>% 
  filter(p.value < 0.05) %>% #pull(estimate) %>% hist
  ggplot(data=.,aes(estimate,fill=dc))+
  geom_histogram(bins=50)


f42 %>% 
  filter(term=='lst') %>% 
  filter(p.value < 0.05) %>% 
  ggplot(data=.,aes(xc,yc,fill=estimate,color=estimate))+
  geom_point(size=0.5)+
  scale_fill_gradient2()+
  scale_color_gradient2(limits=c(-0.1,0.1),oob=scales::squish)+
  labs(color='lst')+
  coord_sf()+
  theme_dark()+
  facet_wrap(~dc,ncol = 1)

all_out %>% 
  filter(term=='Topt') %>% 
  filter(p.value < 0.05) %>% 
  ggplot(data=.,aes(xc,yc,fill=estimate,color=estimate))+
  geom_point(size=0.5)+
  scale_fill_viridis_c(option='B')+
  scale_color_viridis_c(option='B')+
  labs(color='Topt')+
  coord_sf()+
  facet_wrap(~dc,ncol = 1)

all_out %>% 
  filter(term=='Topt') %>% 
  group_by(id) %>% 
  mutate(val = (. %>% filter(dc=="drought") %>% pull(estimate))-
      (. %>% filter(dc=="wet") %>% pull(estimate)))
all_out %>% 
  filter(term=='Topt') %>% 
  filter(is.na(estimate)==F) %>% 
  group_by(id) %>% 
  filter(p.value < 0.05) %>% 
  # filter(any(c("drought","wet") %in% dc)) %>% 
  pivot_wider(
    id_cols=id,
    names_from = dc,
    names_glue = "{dc}",
    values_from = estimate
    ) %>% 
  filter(is.na(wet)==F & is.na(drought)==F) %>% 
  mutate(val = drought - wet) %>% 
  pull(val) %>% summary(100)

#  Mixed forests LC-43 ---------------------------------------------------
f43 <- arrow::read_parquet(("../data_general/proc_sif-optim/mod_fits/lc-43-fits_2021-09-05.parquet"))
setDT(f43)
f43

f43[,.(val=sum(term=='Topt')),by=dc]

f43 %>% 
  filter(term=='Topt') %>% 
  filter(p.value < 0.05) %>% #pull(estimate) %>% hist
  ggplot(data=.,aes(x=dc, estimate))+
  geom_boxplot(outlier.colour = NA)

f43 %>% 
  filter(term=='lst') %>% 
  filter(p.value < 0.05) %>% 
  ggplot(data=.,aes(x=dc, estimate))+
  geom_boxplot(outlier.colour = NA)+
  coord_cartesian(ylim=c(-0.1,0.1))


# Evergreen forests LC-90 ---------------------------------------------------
f90 <- arrow::read_parquet(("../data_general/proc_sif-optim/mod_fits/lc-90-fits_2021-09-06.parquet"))
setDT(f90)
f90

f90 %>% 
  filter(term=='Topt') %>% 
  filter(p.value < 0.05) %>% #pull(estimate) %>% hist
  ggplot(data=.,aes(x=dc, estimate))+
  geom_boxplot(outlier.colour = NA)

f90 %>% 
  filter(term=='lst') %>% 
  filter(p.value < 0.05) %>% 
  ggplot(data=.,aes(x=dc, estimate))+
  geom_boxplot(outlier.colour = NA)+
  coord_cartesian(ylim=c(-0.1,0.1))

f90 %>% 
  filter(term=='Topt') %>% 
  filter(p.value < 0.05) %>% #pull(estimate) %>% hist
  ggplot(data=.,aes(xc,yc,fill=estimate))+
  geom_tile()+
  scale_fill_viridis_c(option='B')+
  coord_sf()+
  labs(x=NULL,y=NULL,fill='Topt')+
  facet_wrap(~dc,ncol=1)+
  theme_linedraw()+
  theme(panel.grid = element_blank())


f90 %>% 
  filter(term=='Topt') %>% 
  filter(p.value < 0.05) %>% #pull(estimate) %>% hist
  ggplot(data=.,aes(estimate,fill=dc))+
  geom_histogram(bins=50)

f90 %>% 
  filter(term=='lst') %>% 
  filter(p.value < 0.05) %>% #pull(estimate) %>% hist
  ggplot(data=.,aes(xc,yc,fill=estimate))+
  geom_tile()+
  scale_fill_gradient2(limits=c(-0.1,0.1),oob=scales::squish)+
  coord_sf()+
  theme_dark()+
  facet_wrap(~dc,ncol=1)

f90 %>% 
  filter(term=='lst') %>% 
  filter(p.value < 0.05) %>% #pull(estimate) %>% hist
  ggplot(data=.,aes(estimate,fill=dc))+
  geom_histogram(bins=50)


f90 %>% 
  filter(term=='lst') %>% 
  filter(p.value < 0.05) %>% 
  ggplot(data=.,aes(xc,yc,fill=estimate,color=estimate))+
  geom_point(size=0.5)+
  scale_fill_gradient2()+
  scale_color_gradient2(limits=c(-0.1,0.1),oob=scales::squish)+
  labs(color='lst')+
  coord_sf()+
  theme_dark()+
  facet_wrap(~dc,ncol = 1)

f90 %>% 
  filter(term=='Topt') %>% 
  filter(p.value < 0.05) %>% 
  ggplot(data=.,aes(xc,yc,fill=estimate,color=estimate))+
  geom_point(size=0.5)+
  scale_fill_viridis_c(option='B')+
  scale_color_viridis_c(option='B')+
  labs(color='Topt')+
  coord_sf()+
  facet_wrap(~dc,ncol = 1)

f90 %>% 
  filter(term=='Topt') %>% 
  group_by(id) %>% 
  mutate(val = (. %>% filter(dc=="drought") %>% pull(estimate))-
      (. %>% filter(dc=="wet") %>% pull(estimate)))

f90 %>% 
  filter(term=='Topt') %>% 
  filter(is.na(estimate)==F) %>% 
  group_by(id) %>% 
  filter(p.value < 0.05) %>% 
  # filter(any(c("drought","wet") %in% dc)) %>% 
  pivot_wider(
    id_cols=id,
    names_from = dc,
    names_glue = "{dc}",
    values_from = estimate
    ) %>% 
  filter(is.na(wet)==F & is.na(drought)==F) %>% 
  mutate(val = drought - wet) %>% 
  pull(val) %>% hist
  summary(100)
