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

ssdat <- datdat[,`:=`(clst = round(lst,1))][,.(ulst = mean(clst,na.rm=T),
                pdsi = mean(pdsi,na.rm=T)), 
  by=.(id,year,month)]

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


fits <- merge(ssdat, fits,
  by=c("id","year","month"))
# Get name of LC ------------------------------------------------------
lc_name <- case_when(lc_class==41~"Decid. Forest", 
                   lc_class==42~"Evergreen Forest",
                   lc_class==43~"Mixed Forest",
                   lc_class==90~"Woody Wetlands") 



fits %>% 
  # filter(month==7) %>% 
  # sample_n(1000) %>% 
  ggplot(data=.,aes(pdsi,estimate))+
  # geom_point()+
  geom_smooth(method='lm')+
  geom_hline(aes(yintercept=mean(estimate)),col='red')+
  facet_wrap(~month,ncol=1)

fits %>% 
  ggplot(data=.,aes(month,estimate,color=factor(year)))+
  geom_point()
