# Description: Fit Topt of SIF by 3-week time windows in an attempt to 
# identify time required for acclimation.
# Author: Sami Rifai
# Date: 2021-10-19
# Notes: 
# lc_class:
# 41: deciduous forest
# 42: evergreen forest 
# 43: mixed forest
# 

pacman::p_load(tidyverse,data.table,mgcv,nls.multstart,arrow,furrr,lubridate)
future::availableCores()
future::availableWorkers()
setwd("/home/sami/srifai@gmail.com/work/research/sif-optim/")
print(getwd())
source("src/R/functions_sif.R")
 
# options -----------------------------------------------------------------
lc_class <- 43
min_nobs <- 10
n_cores <- 20
n_splits <- 20
start_split_idx <- 1

# futures -----------------------------------------------------------------
options(mc.cores=n_cores)

# Data prep ---------------------------------------------------------------
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

dat[,`:=`(week=week(date))]

qdat <- dat[,.(lst = median(lst,na.rm=T)), 
  by=.(id,week,year)]

qdat2 <- dat[,.(lst = median(lst,na.rm=T)), 
  by=.(xc,yc,week,year)]
qdat2 <- qdat2[,`:=`(region = case_when(
                           (xc< -100 & yc< 37.5)==T~"SW",
                           (xc< -100 & yc>= 37.5)==T~"NW", 
                           (xc>= -100 & yc>= 37.5)==T~"NE", 
                          (xc>= -100 & yc< 37.5)==T~"SE"))]

# Load 3-week interval fits -----------------------------------------------
fits <- arrow::read_parquet("../data_general/proc_sif-optim/mod_fits_max_sif_monthly/lc-43-max-sif-monthly-fits_2021-10-19.parquet")

fits <- fits[,`:=`(lc_class = case_when(lc==41~"Decid. Forest",
                                lc==42~"Evergreen Forest", 
                                lc==43~"Mixed Forest"))]

fits <- fits[,`:=`(region = case_when(
                           (xc< -100 & yc< 37.5)==T~"SW",
                           (xc< -100 & yc>= 37.5)==T~"NW", 
                           (xc>= -100 & yc>= 37.5)==T~"NE", 
                          (xc>= -100 & yc< 37.5)==T~"SE"))]
fits[,`:=`(fmonth = month(month,label=T))]


fits[term=='Topt'][p.value<0.05][std.error<1.5][year%in%c(2018:2019)] %>% 
  ggplot(data=.,aes(week_center,estimate))+
  # geom_point(position = 'jitter')+
  geom_smooth()+
  facet_grid(region~year,scales='free')

qdat2[year%between%c(2018:2019)] %>% 
  ggplot(data=.,aes(week,lst))+
  geom_smooth()+
  facet_grid(region~year,scales='free')

merge(fits[term=='Topt'][p.value<0.05][std.error<1.5][,`:=`(week=week_center)], 
  qdat2, 
  by=c("xc","yc","region", "week","year")) %>% 
  .[year%in%c(2018,2019)] %>% 
  select(region,lst,estimate,week,year) %>% 
  melt(., id=c("region","week","year")) %>% 
  ggplot(data=.,aes(week,value,color=variable))+
  geom_smooth(formula=y~s(x,bs='gp',m=c(-1,10)), 
    se=F)+
  facet_grid(region~year,scales='free')


snobs <- merge(fits[term=='Topt'][p.value<0.05][std.error<1.5][,`:=`(week=week_center)], 
  qdat2, 
  by=c("xc","yc","region", "week","year")) %>% 
  as.data.table() %>% 
  .[,.(snobs=.N),
    by='id'] %>%
  .[snobs>20]

vec_ids <- sample(snobs$id,5)

merge(fits[term=='Topt'][p.value<0.05][std.error<1.5][,`:=`(week=week_center)], 
  qdat2, 
  by=c("xc","yc","region", "week","year")) %>% 
  .[id%in%vec_ids] %>% 
  ggplot(data=.,aes(lst,estimate,group=id,color=id))+
  geom_point(alpha=1,size=1)+
  # geom_smooth(se=F)+
  geom_abline(col='red')+
  scale_color_viridis_c(end=0.9)+
  facet_wrap(~id)+
  theme(legend.position = 'none')

merge(fits[term=='Topt'][p.value<0.05][std.error<1.5][,`:=`(week=week_center)], 
  qdat2, 
  by=c("xc","yc","region", "week","year")) %>% 
  # select(xc,yc) %>% unique %>% nrow
  # pull(id) %>% unique %>% length
  .[year%in%c(2018,2019)] %>% 
  ggplot(data=.,aes(lst,estimate,color=region))+
  geom_point(alpha=0.1,size=0.1)+
  geom_smooth(se=F)+
  geom_abline(col='red')

test <- merge(qdat[id==1],
fits[id==1][term=='Topt'][p.value<0.05][std.error<1.5], by=c("id","year"), allow.cartesian = T) %>% 
  .[,`:=`(diff_temp = estimate - lst, 
          diff_time = week_center - week)]


vec_ids <- unique(snobs$id) %>% sample(10)
merge(qdat[id%in%vec_ids],
fits[id%in%vec_ids][term=='Topt'][p.value<0.05][std.error<1.5], by=c("id","year"), allow.cartesian = T) %>% 
  .[,`:=`(diff_temp = estimate - lst, 
          diff_time = week_center - week)] %>% 
  ggplot(data=.,aes(diff_time, sqrt(diff_temp**2) ,color=factor(id)))+
  geom_hline(aes(yintercept=0))+
  geom_vline(aes(xintercept=0))+
  geom_line(lwd=0.1)+
  # geom_point(alpha=0.1,size=0.1)+
  # geom_smooth(se=F, lwd=0.1)+
  # geom_smooth(se=F, lwd=1, 
  #   inherit.aes = F, 
  #   aes(diff_time, sqrt(diff_temp**2)), 
  #   formula=y~s(x,k=40))+
  labs(x='Time Difference (weeks)',
    y='sqrt((Topt - LST[week])**2)')+
  facet_wrap(~id, scales='free')+
  theme(legend.position = 'none')
ggsave("figures/fig_acclim_period_topt-3week_mixed-forest.png", 
  width=20,
  height=12, 
  units='cm',
  dpi=350)

vec_ids <- unique(snobs$id) %>% sample(4)
merge(qdat,
fits[term=='Topt'][p.value<0.05][std.error<1.5], by=c("id","year"), allow.cartesian = T) %>% 
  .[,`:=`(diff_temp = lst - estimate, 
          diff_time = week_center - week)] %>%
  .[,`:=`(part = case_when(week_center <= 23 ~ "early", 
                          between(week_center,24,30)~"mid",
                           week_center >= 31 ~ "late"))] %>% 
  .[,`:=`(region = factor(region,levels = c("NW","NE","SW","SE"), ordered = T), 
          season = factor(part, levels=c("early","mid","late"), ordered = T))] %>% 
  .[diff_time %in% c(-12:12)] %>% 
  ggplot(data=.,aes(diff_time,(diff_temp),color=season))+
  # geom_point(aes(diff_time,abs(diff_temp),group=id),alpha=0.1)+
  geom_hline(aes(yintercept=0),col='black')+
  geom_vline(aes(xintercept=0),col='black')+
  geom_smooth(formula=y~s(x,bs='cs'))+
  scale_color_viridis_d(end=0.9)+
  labs(y=expression(paste(T[opt]-LST)), 
    x="Week(Topt) - LST(week)")+
  facet_wrap(~region,scales='free')+
  theme_linedraw()+
  theme()
  # ggplot(data=.,aes(diff_time, sqrt(diff_temp**2) ,color=factor(id)))+
  

