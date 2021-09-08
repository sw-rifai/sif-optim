# Description: Extracts the values from Earth Engine multibands tiffs and 
# merges the SIF, LST, and PDSI into parquets (1 for each region and LC). 
# The regions have been arbitrary defined to limit the memory consumption.
# Notes: 
# * memory hungry, needs 64-128 Gb ram
# * stars tiff warping & cropping can be finicky and fail silently resulting
# in joins with missing data
# * not parallelized because too many workers demand too much memory

pacman::p_load(tictoc, tidyverse,  
  data.table,lubridate,
  future,foreach, 
  stars)

# OPTIONS ---------------------------------------------------------
lc_class <- 41 # see below for descrip of lc classes
redo <- TRUE # do all the chunks need to be reprocessed?

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

# functions -------------------------------------------
fn_get_dates <- function(fname){
  n <- dim(stars::read_stars(fname,proxy=T))[3]
  d12 <- str_extract_all(fname,pattern = "\\d{4}\\-\\d{2}\\-\\d{2}") %>% 
    unlist() %>% .[1:2] %>% ymd
  out <- seq(d12[1],by='1 day',length.out=n)
  return(out[1:(length(out))])
}
fn_dt_sif <- function (x, ..., add_max = FALSE, center = NA){
  gc()
    data.frame(st_coordinates(x, add_max = add_max, center = center), 
        lapply(x, function(y) structure(y, dim = NULL))) %>% 
    na.omit() %>% 
    as.data.table() %>% 
    .[sif>0]
}
fn_dt_lst <- function (x, ..., add_max = FALSE, center = NA){
  gc()
    data.frame(st_coordinates(x, add_max = add_max, center = center), 
        lapply(x, function(y) structure(y, dim = NULL))) %>% 
    na.omit() %>% 
    as.data.table()
}

fn_get_pdsi_dates <- function(fname){
  d12 <- str_extract_all(fname,pattern = "\\d{4}\\-\\d{2}\\-\\d{2}") %>% 
    unlist() %>% .[1:2] %>% ymd
  dims <- stars::read_stars(fname,proxy=T) %>% dim()
  out <- seq(d12[1],d12[2],length.out=(dims[3]))
  return(out[1:(length(out))])
}

# end section *****************************************

# main -------------------------------------------------
(dunzo <- tibble(fname=list.files("../data_general/proc_sif-optim/merged_parquets/", 
  pattern=as.character(lc_class))) %>%
  mutate(region=str_extract(fname,"region_\\d{1}"),
         year = str_extract(fname,"\\d{4}") %>% as.numeric))

dseq <- expand_grid(region = paste0("region_",1:5), 
            year = 2018:2020)
if(redo==F){dseq <- anti_join(dseq,dunzo,by=c("region","year")) %>%
 mutate(idx = row_number())}
xs <- seq(-124.74006,-90.24475,length.out=6)
region_1 <- st_bbox(c(xmin=xs[1],xmax=xs[2],ymax=49.38937,ymin=24.54197,crs=4326)) %>% st_set_crs(4326)
region_2 <- st_bbox(c(xmin=xs[2],xmax=xs[3],ymax=49.38937,ymin=24.54197,crs=4326)) %>% st_set_crs(4326)
region_3 <- st_bbox(c(xmin=xs[3],xmax=xs[4],ymax=49.38937,ymin=24.54197,crs=4326)) %>% st_set_crs(4326)
region_4 <- st_bbox(c(xmin=xs[4],xmax=xs[5],ymax=49.38937,ymin=24.54197,crs=4326)) %>% st_set_crs(4326)
region_5 <- st_bbox(c(xmin=xs[5],xmax=xs[6],ymax=49.38937,ymin=24.54197,crs=4326)) %>% st_set_crs(4326)
# region_6 <- st_bbox(c(xmin=xs[6],xmax=xs[7],ymax=49.38937,ymin=25,crs=4326)) %>% st_set_crs(4326)
# region_7 <- st_bbox(c(xmin=xs[7],xmax=xs[8],ymax=49.38937,ymin=25,crs=4326)) %>% st_set_crs(4326)
# region_8 <- st_bbox(c(xmin=xs[8],xmax=xs[9],ymax=49.38937,ymin=25,crs=4326)) %>% st_set_crs(4326)
# region_9 <- st_bbox(c(xmin=xs[9],xmax=xs[10],ymax=49.38937,ymin=25,crs=4326)) %>% st_set_crs(4326)
# region_10 <- st_bbox(c(xmin=xs[10],xmax=xs[11],ymax=49.38937,ymin=25,crs=4326)) %>% st_set_crs(4326)
# region_11 <- st_bbox(c(xmin=xs[11],xmax=xs[12],ymax=49.38937,ymin=25,crs=4326)) %>% st_set_crs(4326)
# region_12 <- st_bbox(c(xmin=xs[12],xmax=xs[13],ymax=49.38937,ymin=25,crs=4326)) %>% st_set_crs(4326)

for(idx in 1:dim(dseq)[1]){
 source_path <- paste0("../data_general/proc_sif-optim/sif-subset-by-lc-",lc_class)
 roi_name <- dseq[idx,'region']$region
 i_year <- dseq[idx,'year']$year
 print(paste('starting:',roi_name,i_year))

 roi <- eval(as.name(roi_name))


  flist_sif <- list.files(source_path,
    full.names = T, pattern='SIF')
  flist_sif <- flist_sif[str_detect(flist_sif,as.character(i_year))]
  flist_sif <- flist_sif[str_detect(flist_sif,as.character("0000000000-0000000000"))]
  
  
  if(length(flist_sif)==0){ # GEE didn't split year 2000 sif into two tifs
   flist_sif <- list.files(source_path,
    full.names = T, pattern='SIF')
    flist_sif <- flist_sif[str_detect(flist_sif,as.character(i_year))]
    
  }
  
  s <- stars::read_stars(flist_sif[1],proxy=F) %>% set_names('sif')
  s <- s %>% st_set_dimensions(.,3,values=fn_get_dates(flist_sif[1]),names = 'date')
  s <- st_crop(s,roi,epsilon = 0.999)
  # s <- s[st_as_sfc(roi)]
  st_bbox(s)
  gc(full=T)
  
  
  # doFuture::registerDoFuture()
  gc()
  # options(future.globals.maxSize= floor(1.1*object.size(s)) )
  i_max <- as.numeric(dim(s)[3])
  tic()
  d_s <- foreach(i=1:i_max) %do% {
    gc(full=T)
    out_s <- fn_dt_sif(s[,,,i])
    # out_s <- setkeyv(out_s,cols = c("x","y","date"))
    out_s
  }
  toc()
  gc(full=T)
  rm(s); gc()
  d_s <- rbindlist(d_s)
  print(paste("done: sif"))
  
  # Extract LST -------------------------------------------------------------
  gc(full=T)
  flist_lst <- list.files(source_path,
    full.names = T, pattern='MYD')
  flist_lst <- flist_lst[str_detect(flist_lst,as.character(i_year))]
  flist_lst <- flist_lst[str_detect(flist_lst,as.character("0000000000-0000000000"))]

  if(length(flist_lst)==0){ # GEE didn't split year 2000 sif into two tifs
   flist_lst <- list.files(source_path,
    full.names = T, pattern='MYD')
    flist_lst <- flist_lst[str_detect(flist_lst,as.character(i_year))]
  }

  
  gc(full=T)
  m <- stars::read_stars(flist_lst[1],proxy=F) %>% set_names("lst")
  gc(full=T)
  m <- m %>% st_set_dimensions(.,3,values=fn_get_dates(flist_lst[1]),names = 'date')
  gc(full=T)
  m <- st_crop(m,roi,epsilon = 0.999)
  st_bbox(m)
  
  i_max <- as.numeric(dim(m)[3])
  tic()
  d_m <- foreach(i=1:i_max) %do% {
    gc(full=T)  
    out_m <- fn_dt_lst(m[,,,i])
    # setkeyv(out_m,cols = c("x","y","date"))
    out_m
  }
  toc()
  gc(full=T)
  d_m <- rbindlist(d_m)
  
  # tables()
  setkeyv(d_s,cols = c("x","y","date"))
  gc(full=T)
  setkeyv(d_m,cols = c("x","y","date"))
  gc(full=T)

  print(paste("done: lst"))
  
  dat <- merge(d_s, d_m, all.x = T,all.y=F)
  gc(full=T)
  dat <- dat[lst>0]
  gc(full=T)
  
  # Extract PDSI ------------------------------------------------------------
  gc(full=T)
  flist_p <- list.files(source_path,
    full.names = T, pattern='pdsi')
  flist_p <- flist_p[str_detect(flist_p,as.character(i_year))]
  gc(full=T)
  p <- stars::read_stars(flist_p[1],proxy=F) %>% set_names("pdsi")
  gc(full=T)
  p <- p %>% st_set_dimensions(.,3,
    values=fn_get_pdsi_dates(flist_p[1]),names = 'date')
  gc(full=T)

  n_bands <- dim(p)[3]
  p <- st_warp(p,m[,,,1:n_bands],use_gdal = F)
  p <- st_as_stars(p)

  # tests ***  
  st_get_dimension_values(p,1) %in% 
    st_get_dimension_values(m,1) %>% 
    table()
  st_get_dimension_values(p,2) %in% 
    st_get_dimension_values(m,2) %>% 
    table()
  
  gc(full=T)
  # p <- st_crop(p,roi,epsilon = 0.999)
  d_p <- p %>% as.data.table()
  
  # tests ***
  print(idx)
  unique(d_p$x) %in% st_get_dimension_values(p,1) %>% 
    table()
  unique(d_p$y) %in% st_get_dimension_values(m,2) %>% 
    table()
  unique(d_p$y) %in% unique(dat$y) %>% 
    table()

  rm(m)
  
  setkeyv(d_p,cols=c("x","y","date"))
  gc(full=T)

  print(paste("done: pdsi"))

  print(paste("LOOK FOR THE TESTS"))

  dat <- d_p[dat,roll=T]
  
  # tests ***
  
  print(paste("n missing pdsi", sum(is.na(dat$pdsi))))

  (fname_out <- paste0("lc-",lc_class,"_",roi_name,"sif_","lst_","1km_",i_year,"_.parquet"))
  print(fname_out)
  arrow::write_parquet(dat,sink = paste0("../data_general/proc_sif-optim/merged_parquets/",
    fname_out),compression = 'snappy')
  rm(dat,d_m,d_p,d_s,p)
  gc(full=T)

}
# gc(full=T)
# flist_p <- list.files("../data_general/proc_sif-optim/sif-subset-by-lc/",
#   full.names = T, pattern='pdsi')
# gc(full=T)
# p <- stars::read_stars(flist_p[1],proxy=F) %>% set_names("pdsi")
# gc(full=T)
# p <- p %>% st_set_dimensions(.,3,values=fn_get_pdsi_dates(flist_p[1]),names = 'date')
# gc(full=T)
# p <- st_crop(p,region_1,epsilon = 0.999)
# p <- p %>% as.data.table()
# setkeyv(p,cols=c("x","y","date"))
# gc(full=T)
# 
# 
# dat <- arrow::read_parquet("../data_general/proc_sif-optim/merged_parquets/region_1sif_lst_1km_2018_.parquet")
# 
# dat <- p[dat,roll=T]
# dat$pdsi %>% summary
# unique(p$x) %in% unique(dat$x)
# unique(p$y) %in% unique(dat$y)
# summary(p$y)
# summary(dat$y)
# # 
# # 
# # 
