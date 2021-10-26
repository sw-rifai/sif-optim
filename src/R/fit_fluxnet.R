pacman::p_load(tidyverse,data.table,stars,lubridate)
library(ncdf4)
library(tidync)

flist <- list.files("../data_general/fluxnet/fluxnet2015_proc/Daily/","US",full.names = T)
vec_sites <- substr(flist,49,54)

fn <- function(site_name){
 f_paths <- flist[str_detect(flist,site_name)]
 f_met <- f_paths[str_detect(f_paths,"Flux.nc")]
 f_flux <- f_paths[str_detect(f_paths,"Met.nc")]
 
 # o1 <- tidync::tidync(f_met) %>% hyper_tibble()
 # o2 <- tidync::tidync(f_flux) %>% hyper_tibble()
  o1 <- stars::read_ncdf(f_met,make_time = T,make_units = F) %>% 
  as.data.table()
  o2 <- stars::read_ncdf(f_flux,make_time = T,make_units = F) %>% 
  as.data.table()
  o <- merge(o1,o2,by='time')
  return(o)
}

# Scratch ------------
test <- fn(flist[1])
test %>% glimpse
tidync(flist[1]) %>% hyper_dims()
tidync(flist[1]) %>% hyper_grids()
tidync(flist[1]) %>% hyper_tibble()


tidync::tidync(flist[2])

o <- fn(vec_sites[1])
o <- o %>% 
  mutate(date = time) %>% 
  mutate(lst = Tair-273.15) %>% 
  mutate(month=month(date)) %>% 
  mutate(year = year(date)) %>% 
  filter(month %in% c(5:9)) %>% 
  filter(GPP >0 )
o %>% 
  ggplot(data=.,aes(lst, GPP))+
  geom_point()+
  geom_smooth()+
  facet_grid(year~month)


nls.multstart::nls_multstart(GPP ~ kopt * ((Hd * (2.718282^((Ha*(lst-Topt))/(lst*0.008314*Topt)))) / 
                                (Hd - (Ha*(1-(2.718282^((Hd*(lst-Topt))/(lst*0.008314*Topt))))))),
                         data = o %>% filter(year==2011&month==9),
                         iter = 100,
                         start_lower = c(kopt = 2, Hd = 200, Ha = 50, Topt = 15),
                         start_upper = c(kopt = 10, Hd = 200, Ha = 300, Topt = 35),
                         supp_errors = 'Y',
                         na.action = na.omit,
                         #convergence_count = 500,
                         lower = c(kopt = 0.1, Hd = 0, Ha = 0, Topt = 0)) %>% 
  summary



# PLUMBER2 ----------------------------------------------------------------
flist_met <- list.files("../data_general/fluxnet/PLUMBER2/Post-processed_PLUMBER2_outputs/Nc_files/Met/","US",full.names = T)
flist_flux <- list.files("../data_general/fluxnet/PLUMBER2/Post-processed_PLUMBER2_outputs/Nc_files/Flux/","US",full.names = T)
flist_flux
vec_sites <- substr(flist_flux,81,86)
enf <- c("US-Blo","US-Blo","US-GLE","US-Ho1","US-Me2","US-Me4","US-Me6",
         "US-MOz","US-NR1","US-SP1","US-SP2","US-SP3")
dbf <- c("US-Ha1","US-MMS","US-UMB","US-WCr")
csh <- c("US-KS2")
mf <- c("US-PFa","US-Syv")
wsa <- c("US-Ton")
osh <- c("US-Whs")

fn_prep <- function(site_name){
 f_met <- flist_met[str_detect(flist_met,site_name)]
 f_flux <- flist_flux[str_detect(flist_flux,site_name)]
 
 # o1 <- tidync::tidync(f_met) %>% hyper_tibble()
 # o2 <- tidync::tidync(f_flux) %>% hyper_tibble()
  o1 <- stars::read_ncdf(f_met,make_time = T,make_units = F) %>% 
  as.data.table()
  o2 <- stars::read_ncdf(f_flux,make_time = T,make_units = F) %>% 
  as.data.table()
  o <- merge(o1,o2,by='time')
  o <- o %>% 
    mutate(date=time) %>% 
    mutate(year=year(date), 
      month=month(date), 
      hour=hour(date)) %>% 
    filter(between(hour,10,15)) %>% 
    filter(GPP>0) %>% 
    mutate(TairC = Tair-273.15)
  norms <- o %>% 
    group_by(month) %>% 
    summarize(sw80 = quantile(SWdown,0.80,na.rm=T), 
              Ta90 = quantile(TairC,0.90,na.rm=T)) %>% 
    ungroup()
  o <- inner_join(o,norms)
  o$site <- site_name
  return(o)
}

fn_quant <- function(din){
  out <- din[SWdown>80][
    is.na(GPP)==F
  ][
    is.na(TairC)==F
  ][
    month%in%c(5:9)
  ][,`:=`(TaC = round(TairC*2)/2)][
    ,.(gpp_lai90 = quantile(GPP/LAI,0.9),
       gpp90 = quantile(GPP,0.9),
       tmin = min(TairC),
      tmax = max(TairC),
      tmean = mean(TairC)),by=.(site,year,month,TaC)
  ]
    # out <- din %>% 
  # filter(SWdown > sw80) %>% 
  # filter(is.na(GPP)==F) %>% 
  # filter(is.na(TairC)==F) %>% 
  # filter(month%in%c(5:9)) %>% 
  # mutate(TaC = round(TairC*2)/2) %>% #pull(TaC)
  # group_by(site,year,month,TaC) %>%
  # summarize(gpp_lai90 = quantile(GPP/LAI,
  #   0.9,
  #   na.rm=T), 
  #    tmin = min(TairC),
  #    tmax = max(TairC),
  #    tmean = mean(TairC)) %>% 
  # ungroup()
  return(out)
}

d1 <- lapply(enf,fn_prep)
d2 <- lapply(d1,fn_quant)

dat <- fn_prep(enf)
d2 <- fn_quant(dat)

all_out[[1]] <- d2 %>%
                split(f=list(.$id,.$year,.$month),
                  drop=T) %>%
                map(~parab_dt_monthly(.x)) %>%
    rbindlist()


dat %>% 
  ggplot(data=.,aes(date,LAI))+
  geom_point()


dat %>% 
  filter(SWdown > sw80) %>% 
  # sample_n(10000) %>% 
  filter(is.na(GPP)==F) %>% 
  filter(is.na(TairC)==F) %>% 
  filter(month%in%c(5:9)) %>% 
  mutate(TaC = round(TairC*2)/2) %>% #pull(TaC)
  group_by(year,month,TaC) %>%
  summarize(val = quantile(GPP/LAI,
    0.9,
    na.rm=T)) %>% 
  ungroup() %>% 
  ggplot(data=.,aes(TaC,val))+
  geom_point()+
  geom_smooth(method='nls',
    formula=y~p_opt-b*(x-Topt)**2,
    se=F,
    method.args=list(
      start=c(p_opt=10,b=1,Topt=25)
      # algorithm='port'
    ))+
  geom_vline(aes(xintercept=20),col='red')+
  facet_grid(~month,scales='free')

o %>% 
  filter(SWdown>900) %>% 
  filter(month%in%c(5:9)) %>% 
  # sample_n(10000) %>% 
  ggplot(data=.,aes(VPD,GPP))+
  geom_point()+
  geom_smooth()+
  facet_wrap(~month)

o %>% 
  # filter(SWdown>900) %>% 
  filter(month%in%c(5:9)) %>% 
  # sample_n(10000) %>% 
  ggplot(data=.,aes(TairC,SWdown))+
  geom_point()+
  geom_smooth()+
  facet_wrap(~month)
o %>% 
  filter(month%in%c(5:9)) %>% 
  ggplot(data=.,aes(LAI_alternative,GPP))+
  geom_point()+
  geom_smooth()+
  facet_grid(year~month)

q <- o %>% 
  mutate(TaC = round(4*TairC)/4) %>% 
  group_by(year,month,hour,TaC) %>% 
  summarize(gpp95 = quantile(GPP,0.95,na.rm=T),
            gpp50 = median(GPP,na.rm =T),
            gpp_max = max(GPP,na.rm=T),
            vpd50 = median(VPD*0.1,na.rm=T)) %>% 
  ungroup()

q %>% 
  filter(month %in% c(5,6,7,8,9)) %>% 
  ggplot(data=.,aes(vpd50,gpp_max))+
  geom_point()+
  geom_smooth()+
    geom_smooth(method='nls',
    formula=y~p_opt-b*(x-Topt)**2,
    se=F,
    color='red',
    method.args=list(
      start=c(p_opt=10,b=1,Topt=25)
      # algorithm='port'
    ))+
  facet_grid(year~month,scales='free')


q %>% 
  filter(month %in% c(5,6,7,8,9)) %>% 
  ggplot(data=.,aes(TaC,gpp_max))+
  geom_point()+
  geom_smooth()+
  facet_grid(hour~month,scales='free')

o %>% 
  mutate(date=time) %>% 
  mutate(year=year(date), 
    month=month(date), 
    hour=hour(date)) %>% 
  ggplot(data=.,aes(hour, GPP))+
  geom_point()+
  geom_smooth()


all_out <- list()

all_out[[1]] %>% 
  filter(term=='Topt') %>% 
  inner_join(.,q2 %>% 
      select(tmax,tmean,tmin,year,month) %>% distinct(),
    ,by=c("year","month")) %>% 
  ggplot(data=.,aes(tmean,estimate))+
  geom_point()+
  geom_smooth(method='lm')


all_out[[1]][term=='TaC']
all_out[[1]][term=='Topt']

setDT(q)
q[year==2001][month%in%c(5,6,7,8,9)] %>% 
  ggplot(data=.,aes(TaC,gpp,color=factor(month)))+
  geom_point()+
  # geom_smooth()+
  geom_smooth(method='nls',
    formula=y~p_opt-b*(x-Topt)**2,
    se=F,
    # color='red',
    method.args=list(
      start=c(p_opt=10,b=1,Topt=25)
      # algorithm='port'
    ))+
  scale_color_viridis_d()


