# Description: Fit Topt or linear mods for max SIF per unit leaf area
# Author: Sami Rifai
# Date: 2021-10-20
# Notes: DO NOT RUN IN RSTUDIO. Not even with the jobs function.
#  Run on command line with following:
#  Rscript --no-save --no-restore --verbose src/R/fit_max-sif-dLAI_pa-lm.R > outputFile.Rout 2>&1
# * memory hungry, needs 60+ gb
# * Still mysteriously fails sometimes, but this is likely caused by furrr
#   starting too many workers thank bankrupt the memory. 
# lc_class:
# 41: deciduous forest
# 42: evergreen forest 
# 43: mixed forest
# 

pacman::p_load(tidyverse,data.table,mgcv,nls.multstart,arrow,furrr,lubridate, 
  stars)
future::availableCores()
future::availableWorkers()
setwd("/home/sami/srifai@gmail.com/work/research/sif-optim/")
print(getwd())
source("src/R/functions_sif.R")
 
# options -----------------------------------------------------------------
lc_class <- 43
min_nobs <- 100
n_cores <- 20
n_splits <- 20
start_split_idx <- 1

# futures -----------------------------------------------------------------
options(mc.cores=n_cores)

# SIF Data prep ---------------------------------------------------------------
if(file.exists(paste0("../data_general/proc_sif-optim/parquet-by-lc/sif_lst_pdsi_lc",lc_class,".parquet"))==F){
  dat <- lapply(
    list.files("../data_general/proc_sif-optim/merged_parquets/",pattern = paste0("lc-",lc_class),full.names = T),
    arrow::read_parquet)
  dat <- rbindlist(dat)
  dat[,lc := lc_class]
  dat[,`:=`(lst = lst*0.02 - 273.15, 
            sif = sif/1000)][,`:=`(xc=round(x*4)/4,
    yc=round(y*4)/4)][,`:=`(month=month(date),
      year=year(date))]
  g <- unique(dat[,.(xc,yc)])[,.(id=.GRP),by=.(xc,yc)]
  dat <- merge(dat,g,by=c("xc","yc"))
  dat[,`:=`(fid = factor(id))]
  dat[,`:=`(dc = case_when(pdsi <= -2 ~ "drought",
                           pdsi > -2 & pdsi < 2 ~ "norm",
                           pdsi >= 2 ~ "wet"))]
  
  
  nobs <- dat[,.(nobs=.N),by=.(id,dc)]
  dat <- merge(dat,nobs,by=c("id","dc"))
  dat <- dat[nobs >= min_nobs]
  
  arrow::write_parquet(dat,
    sink=paste0("../data_general/proc_sif-optim/parquet-by-lc/sif_lst_pdsi_lc",lc_class,".parquet"),compression = 'snappy')
}else{
  dat <- arrow::read_parquet(paste0("../data_general/proc_sif-optim/parquet-by-lc/sif_lst_pdsi_lc",lc_class,".parquet"))
}

coords <- unique(dat[,.(x,y)])

# 2018 LAI data prep -----------------------------------------------------------
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

l_2018 <- merge(l1,coords,by=c("x","y"),all.x = F, all.y=T)


# 2019 LAI ----------------------------------------------------------------
l1 <- stars::read_stars(
  list.files("../data_general/proc_sif-optim/conus_lai/",'2019',
    full.names = T)[1], proxy = T) %>% 
  st_set_dimensions(.,3, 
 values=seq(ymd("2019-05-01"),ymd("2019-09-29"),by='4 days'),names = 'date') %>% 
  set_names("lai") %>% 
  as.data.table() %>% 
  .[lai>0]
l1 <- merge(l1,coords,by=c("x","y"),all.x = F, all.y=T)
gc(full=T)

l2 <- stars::read_stars(
  list.files("../data_general/proc_sif-optim/conus_lai/",'2019',
    full.names = T)[2], proxy = T) %>% 
  st_set_dimensions(.,3, 
 values=seq(ymd("2019-05-01"),ymd("2019-09-29"),by='4 days'),names = 'date') %>% 
  set_names("lai") %>% 
  as.data.table() %>% 
  .[lai>0]
l2 <- merge(l2,coords,by=c("x","y"),all.x = F, all.y=T)
gc(full=T)

l3 <- stars::read_stars(
  list.files("../data_general/proc_sif-optim/conus_lai/",'2019',
    full.names = T)[3], proxy = T) %>% 
  st_set_dimensions(.,3, 
 values=seq(ymd("2019-05-01"),ymd("2019-09-29"),by='4 days'),names = 'date') %>% 
  set_names("lai") %>% 
  as.data.table() %>% 
  .[lai>0]
l3 <- merge(l3,coords,by=c("x","y"),all.x = F, all.y=T)
gc(full=T)

l_2019 <- rbindlist(list(l1,l2,l3))
rm(l1,l2,l3); gc(full=T)


# 2020 LAI ----------
ref <- st_as_stars(coords,dims=c("x","y"))
st_crs(ref) <- st_crs(4326)

l1 <- stars::read_stars(
  list.files("../data_general/proc_sif-optim/conus_lai/",'2020',
    full.names = T)[1], proxy = T) %>% 
  st_set_dimensions(.,3, 
 values=seq(ymd("2020-05-01"),by='4 days',length.out=15),names = 'date') %>% 
  set_names("lai")
l1 <- l1 %>% st_warp(., ref, use_gdal = F)
l1 <- l1 %>% as.data.table() %>% 
                     .[lai>0]
l1 <- l1[coords, on=c("x","y"), nomatch=0]
gc(full=T)

l2 <- stars::read_stars(
  list.files("../data_general/proc_sif-optim/conus_lai/",'2020',
    full.names = T)[2], proxy = T) %>% 
  st_set_dimensions(.,3, 
 values=seq(ymd("2020-05-01"),by='4 days',length.out=15),names = 'date') %>% 
  set_names("lai")
l2 <- l2 %>% st_warp(., ref, use_gdal = F)
l2 <- l2 %>% as.data.table() %>% 
                     .[lai>0]
l2 <- l2[coords, on=c("x","y"), nomatch=0]
gc(full=T)

l3 <- stars::read_stars(
  list.files("../data_general/proc_sif-optim/conus_lai/",'2020',
    full.names = T)[3], proxy = T) %>% 
  st_set_dimensions(.,3, 
 values=seq(ymd("2020-05-01"),by='4 days',length.out=15),names = 'date') %>% 
  set_names("lai")
l3 <- l3 %>% st_warp(., ref, use_gdal = F)
l3 <- l3 %>% as.data.table() %>% 
                     .[lai>0]
l3 <- l3[coords, on=c("x","y"), nomatch=0]
gc(full=T)

l_2020 <- rbindlist(list(l1,l2,l3))
# rm(l1,l2,l3); gc(full=T)

dlai <- rbindlist(list(l_2018,l_2019,l_2020),fill=T)

# join ------------
s_lai <- dlai[,`:=`(month=month(date), 
  year=year(date))][,.(lai = mean(lai,na.rm=T)/10), 
    by=.(x,y,month,year)]
setkeyv(s_lai, cols=c("x","y","year","month"))

setkeyv(dat, cols=c("x","y","year","month"))

s_lai <- s_lai[is.na(lai)==F]
# dat2 <- dat[s_lai, roll='nearest']
dat2 <- merge(dat,s_lai, by=c("x","y","year","month"))

sdat <- dat2[,`:=`(clst = round(lst,1))][
  ,.(sif_lai = max(sif/lai),
    sif = max(sif),
    lai = median(lai)),
  by=.(xc,yc,clst,year,month)]
sdat[,`:=`(id = .GRP), by=.(xc,yc)]
setnames(sdat,'clst','lst')
# setnames(sdat,'fid','id')
snobs <- sdat[,.(nobs = .N),by=.(id,year,month)]
sdat <- merge(sdat,snobs,by=c('id','year','month'))
sdat <- sdat[nobs>=min_nobs]
sdat <- sdat[,`:=`(proc_id = .GRP), by=.(id,year,month)]


# scratch ---------------------------------------------
vec_ids <- sdat$id %>% unique()

sdat[id%in%sample(vec_ids,5)] %>% 
  ggplot(data=.,aes(lst,sif_lai))+
    geom_point()+
      geom_smooth(method='nls',
    formula=y~p_opt-b*(x-Topt)**2,
    se=F,
    color='red',
    method.args=list(
      start=c(p_opt=10,b=1,Topt=25)
      # algorithm='port'
    ))+
    geom_smooth(method='nls',
      inherit.aes = F,
      aes(lst,sif),
    formula=y~p_opt-b*(x-Topt)**2,
    se=F,
    color='blue',
    method.args=list(
      start=c(p_opt=10,b=1,Topt=25)
      # algorithm='port'
    ))+
  facet_wrap(~id+year+month)

sdat <- sdat[,`:=`(region = case_when(
                           (xc< -100 & yc< 37.5)==T~"SW",
                           (xc< -100 & yc>= 37.5)==T~"NW", 
                           (xc>= -100 & yc>= 37.5)==T~"NE", 
                          (xc>= -100 & yc< 37.5)==T~"SE"))]

sdat[id%in%sample(vec_ids,100)] %>% 
  ggplot(data=.,aes(lai,sif,color=factor(id)))+
  geom_point(alpha=0.1)+
  geom_smooth(se=F,inherit.aes = F, 
    aes(lai,sif))+
  scale_color_viridis_d(option='H')+
  facet_wrap(~region)+
  theme(legend.position = 'none')

junk <- s_lai[x==dat2[fid==1][,.(x,y)][1,]$x][
  y==dat2[fid==1][,.(x,y)][1,]$y
]
junk %>% ggplot(data=.,aes(week,lai,color=factor(year)))+geom_line()

snobs <- dat2[,.(nobs = sum(is.na(lai)==F&is.na(sif)==F)), 
  by=.(fid)]
dat[fid==5034] %>% 
  ggplot(data=.,aes(date,sif,color=factor(year)))+geom_line()+
  facet_wrap(~year,scales='free')
dat2[fid==5034] %>% 
  ggplot(data=.,aes(date,sif/lai,color=factor(year)))+geom_line()+
  facet_wrap(~year,scales='free')

s_lai[]
dat2[fid==3] %>% pull(lai) 
  ggplot(data=.,aes(date,lai))+
  geom_point()


l1 <- l1[,`:=`(region = case_when(
                           (x< -100 & y< 37.5)==T~"SW",
                           (x< -100 & y>= 37.5)==T~"NW", 
                           (x>= -100 & y>= 37.5)==T~"NE", 
                          (x>= -100 & y< 37.5)==T~"SE"))]

l1[,.(val = mean(lai,na.rm=T)), 
  by=.(region,date)] %>% 
  ggplot(data=.,aes(date,val,color=paste(region)))+
  geom_line()
l1$date %>% min

dat[between(x,-80,-70)][,`:=`(week=week(date))][,.(sif = mean(sif,na.rm=T), 
  lai=mean(lai,na.rm=T)), 
  by=.(year,week)] %>% 
  ggplot(data=.,aes(week,lai,color=factor(year)))+
  geom_line()

dat[between(x,-80,-70)][,.(sif = mean(sif,na.rm=T), 
  lai=mean(lai,na.rm=T)), 
  by=.(x,y,year,month)] %>% 
  ggplot(data=.,aes(x,y,color=lai))+
  geom_point(size=0.1)+
  scale_color_viridis_c()+
  coord_equal()+
  facet_grid(year~month)

sort(unique(l1$date)) %>% diff
l1[date==ymd("2018-05-01")] %>% 
  ggplot(data=.,aes(x,y,fill=lai))+
  geom_tile()+
  scale_fill_viridis_b()+
  coord_sf()
dat[date==ymd("2018-05-01")] %>% 
  ggplot(data=.,aes(x,y,fill=sif))+
  geom_tile()+
  scale_fill_viridis_b()+
  coord_sf()

vec_x <- unique(dat$x)
unique(l1$x) %in% vec_x %>% table

junk
fn <- function(din, y){
  out <- lm(rlang::parse_expr(paste(y,"~week")), data=din)
  return(out)
}
fn(junk,'lai')

var <- sym('lai')
lm(sym('lai')~week, data=junk)
lm(rlang::parse_expr('lai~week'), data=junk)


sdat[sample(.N,10000)][order(pdsi)] %>% 
  ggplot(data=.,aes(lai, sif,color=pdsi))+
  geom_point(size=0.1)+
  scale_color_viridis_c(direction = -1,option='B')
