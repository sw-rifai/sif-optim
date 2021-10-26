pacman::p_load(tidyverse,data.table,stars,lubridate, furrr)
source("src/R/functions_sif.R")

# ameriflux ----------------------------------------------------------------
flist <- list.files("/home/sami/srifai@gmail.com/work/research/data_general/fluxnet/ameriflux/", 
  recursive = T,full.names = T,pattern = "_HH_")

# str_extract(flist,"AMF\\_US\\-\\b[a-zA-Z0-9]{3}") # THIS IS THE REGEX TO EXTRACT SITE!

fn_prep_ameriflux <- function(fname){
  raw <- fread(fname, na.strings = "-9999")
  var_names <- names(raw) %>% sort
  response_name <- c("GPP_PI","GPP_PI_F","NEE_PI","NEE_PI_F")[c("GPP_PI","GPP_PI_F","NEE_PI","NEE_PI_F")%in%var_names]
  response_name <- na.omit(response_name)
  # if(all(c("GPP_PI","NEE_PI")%in%var_names)==FALSE){return(NULL)}
  if(("GPP_PI"%in%var_names)==F & 
      ("GPP_PI_F"%in%var_names)==F &
      ("NEE_PI"%in%var_names)==F & 
      ("NEE_PI_F"%in%var_names)==F){
    return(NULL)
  }
  response <- switch (response_name[1],
     GPP_PI = (raw$GPP_PI),
     GPP_PI_F = (raw$GPP_PI_F),
     NEE_PI = -(raw$NEE_PI),
     NEE_PI_F = -(raw$NEE_PI_F)
   )
 raw$response <- response
 raw$response_name <- response_name[1]
 raw <- raw[is.na(response)==F]

  raw[,`:=`(date = ymd_hm(TIMESTAMP_START))]
  raw[,`:=`(year=year(date),
    month=month(date),
    hour=hour(date))]
  raw <- raw[month%in%5:9]

  if(  is.null(var_names[like(var_names, "SW_IN*")])==T & 
        is.null(var_names[like(var_names, "PPFD_IN*")])==T
    ){
    return(NULL)
  }
  
  if(is.null(var_names[like(var_names, "PPFD_IN*")])==F){
    light_var <- var_names[like(var_names, "PPFD_IN*")][1]
    sw_q <- raw[light_var > 0][,.(sw80 = quantile(get(light_var),0.8,na.rm=T)),by=month]
    raw <- merge(raw,sw_q)[light_var>sw80]
  }else if(is.null(var_names[like(var_names, "SW_IN*")])==F ){
    light_var <- var_names[like(var_names, "SW_IN*")][1]
    sw_q <- raw[light_var > 0][,.(sw80 = quantile(get(light_var),0.8,na.rm=T)),by=month]
    raw <- merge(raw,sw_q)[light_var>sw80]
  }
  # if(("SW_IN"%in%var_names)==F & 
  #     ("SW_IN_1_1_1"%in%var_names)==F &
  #     ("PPFD_IN"%in%var_names)==F & 
  #     ("PPFD_IN_1"%in%var_names)==F){
  #   return(NULL)
  # }
  # 
  # if("SW_IN"%in%names(raw)){
  #   sw_q <- raw[SW_IN > 0][,.(sw80 = quantile(SW_IN,0.8,na.rm=T)),by=month]
  #   raw <- merge(raw,sw_q)[SW_IN>sw80]
  # }else if( ("SW_IN_1_1_1"%in%names(raw)) & ("SW_IN"%in%names(raw)==F)){
  #   sw_q <- raw[SW_IN_1_1_1 > 0][,.(sw80 = quantile(SW_IN_1_1_1,0.8,na.rm=T)),by=month]
  #   raw <- merge(raw,sw_q)[SW_IN_1_1_1>sw80]
  # }else{
  #   sw_q <- raw[is.na(PPFD_IN)==F][,.(sw80 = quantile(PPFD_IN,0.8,na.rm=T)),by=month]
  #   raw <- merge(raw,sw_q)[PPFD_IN>sw80]
  # }
  # raw <- merge(raw,sw_q)[SW_IN_F_MDS>sw80]
  # 
  if("T_SONIC"%in%names(raw)){
  raw <- raw[T_SONIC > -999][T_SONIC > -99]
  raw[,TaC := round(T_SONIC*4)/4]
  }
  if("TA"%in%var_names){
  raw <- raw[TA > -99]
  raw[,TaC := round(TA*4)/4]
  }
  q_ta <- raw[
    ,.(tmin = min(TaC),
      tmax = max(TaC),
      tmean = mean(TaC)),by=.(year,month)
  ]
  
 if("GPP_NT_CUT_REF" %in% names(raw)){
  o <- raw[,.(response90 = quantile(response,0.90)),by=.(year,month,TaC)]
 }else{
  o <- raw[,.(response90 = quantile(response,0.90)),by=.(year,month,TaC)]
 }
 site <- str_extract(fname,"AMF\\_US\\-\\b[a-zA-Z0-9]{3}")
 o$site <- site
 o$response_name <- response_name[1]
 o$light_var <- light_var
 # o$tair_var <- tair_var
 o <- merge(o,q_ta,by=c("year","month"))
 return(o)
}

# fname <- flist[230]
# fn_prep_ameriflux(fname)
# scratch, delete
# for(i in 1:length(flist)){
#   print(i)
#   out <- tryCatch(fn_prep_oneflux(flist[i]), error=function(e) NULL)
#   print(out)
# }

plan(multisession,workers=5)
d1 <- future_map(flist, possibly(fn_prep_ameriflux,NA)) # map pre-processing function
d1 <- d1[is.na(d1)==F] # Drop oneflux sites missing key variables
d2 <- rbindlist(d1)
d3 <- d2 %>%
        split(f=list(.$site,.$year,.$month),
          drop=T) %>% 
        future_map(~parab_dt_monthly(.x))
d4 <- rbindlist(d3)


d5 <- merge(d4,unique(d2[,.(site,year,month,tmin,tmax,tmean)]),
  all.x=T,
  all.y=F,
  by=c("site","year","month"), allow.cartesian = T)
arrow::write_parquet(d5,paste0(
  "../data_general/proc_sif-optim/mod_fits/fluxnet2015_oneflux/fluxnet2015-oneflux_topt-fits_",Sys.Date(),".parquet"))
# d5 <- arrow::read_parquet("../data_general/proc_sif-optim/mod_fits/fluxnet2015_oneflux/fluxnet2015-oneflux_topt-fits_2021-09-27.parquet")
# d5[term=='rsq']

gm <- fread("../data_general/proc_sif-optim/fluxnet_ee_exports/gridmet_pdsi_ameriflux_forest_sites_1995_2021.csv")
setnames(gm,old=c("Site Id","mean"),new=c("site","pdsi"))
gm <- gm[,.(site,date,pdsi)]
gm[,`:=`(year=year(date),month=month(date))]
gm <- gm[,.(pdsi = mean(pdsi,na.rm=T)),by=.(site,year,month)]
setkeyv(gm,cols=c("site","year","month"))
d5$site <- str_remove(d5$site,"FLX_")
d6 <- merge(d5,gm,by=c("site","year","month"))

d6[is.na(pdsi)==T]$site %>% unique

d6[term=='Topt'][p.value<0.05][std.error<5] %>% 
  ggplot(data=.,aes(tmean,estimate,color=pdsi))+
  geom_point()+
  geom_abline(col='red',lty=2)+
  geom_smooth(method='lm',se=F)+
  coord_equal()+
  scale_color_gradient2()+
  facet_wrap(~month)

d6[term=='Topt'][p.value<0.05][std.error < 5] %>% 
  ggplot(data=.,aes(tmean,estimate,color=pdsi))+
  geom_abline(col='#cf0000',lty=1,lwd=1)+
  geom_segment(aes(x=tmin,xend=tmax,y=estimate,yend=estimate,group=site),
    color='grey60',lwd=0.15)+
    geom_point()+
  geom_smooth(method='lm', se=F, inherit.aes = F,
    aes(tmean,estimate),color='grey30',lwd=2)+
  # geom_smooth(method='lm', se=F)+
  scico::scale_color_scico(palette = 'roma',limits=c(-5,5),oob=scales::squish)+
  # scico::scale_color_scico_d(palette = 'batlow',end=0.8,begin=0.1)+
  labs(x='Mean Air Temp. during peak SW (°C)',
    y=expression(paste(T[opt],"(°C)")),
    title='Ameriflux2015 US sites',
    color='PDSI')+
  facet_wrap(~month)+
  theme_linedraw()+
  theme(panel.grid = element_blank())
ggsave(
  filename = "figures/prelim_allSites_Topt_Trange_oneflux-ameriflux2015.png",
  width=20,
  height=15,
  units='cm',
  dpi=300)

d6[term=='Topt'][p.value<0.05][std.error < 5] %>% 
  ggplot(data=.,aes(tmean,estimate,group=site,color=factor(year)))+
  geom_abline(col=c("#000000"),lty=2)+
  geom_point()+
  geom_path()+
  scale_color_viridis_d(option='H')+
  labs(x='Mean Air Temp. during peak SW (°C)',
    y=expression(paste(T[opt],"(°C) of GPP or NEE")),
    title='Ameriflux2015 US sites',
    color='Year')+
  facet_wrap(~site)+
  theme_linedraw()+
  theme(panel.grid = element_blank())
ggsave(
  filename = "figures/prelim_bySites_Topt-Trange-path_ameriflux.png",
  width=20,
  height=15,
  units='cm',
  dpi=300)



library(lme4) # pdsi loses its effect when site is used as a random effect
lmer(estimate~tmean+pdsi+(tmean|month) + (1|site),
  data=d6[term=='Topt'][p.value<0.05][std.error<5]) %>% 
  summary

library(mgcv)
gam(estimate~tmean+pdsi+s((site),bs='re')+s(month,bs='re'),
  data=d6[term=='Topt'][p.value<0.05][std.error<5] %>% 
    mutate(site=factor(site), 
      month=factor(month))) %>% 
  summary

# 
# 
# 
#  %>% 
#   fread()
# dat[,`:=`(date = ymd_hm(TIMESTAMP_START))]
# dat[,`:=`(year=year(date),
#   month=month(date),
#   hour=hour(date))]
# 
# sw_q <- dat[,.(sw80 = quantile(SW_IN_F_MDS,0.8,na.rm=T)),by=month]
# 
# dat <- merge(dat,sw_q)[SW_IN_F_MDS>sw80]
# dat <- dat[TA_F_MDS > -99]
# dat[,TaC := round(TA_F_MDS*4)/4]
# qdat <- dat[,.(gpp90 = quantile(GPP_NT_CUT_REF,0.90)),by=.(year,month,TaC)]
# 
# parab_dt_monthly(qdat)
# 
# qdat[month%in%5:9] %>% 
#   ggplot(data=.,aes(tair_c,gpp))+
#   geom_point()+
#   geom_smooth(method='nls',
#     formula=y~p_opt-b*(x-Topt)**2,
#     se=F,
#     method.args=list(
#       start=c(p_opt=10,b=1,Topt=25)
#       # algorithm='port'
#     ))+
#   # stat_summary(fun = "median", colour = "red", size = 2, geom = "point")+ 
#   # geom_vline(aes(xintercept=after_stat(mean(tair_c))))+
#   facet_grid(year~month)

fname <- flist[222]
fn_prep_ameriflux(fname) %>% 
  ggplot(data=.,aes(TaC,response90))+
  geom_point()+
  geom_smooth(method='nls',
    formula=y~p_opt-b*(x-Topt)**2,
    se=F,
    method.args=list(
      start=c(p_opt=10,b=1,Topt=25)
      # algorithm='port'
    ))+
  # stat_summary(fun = "median", colour = "red", size = 2, geom = "point")+
  # geom_vline(aes(xintercept=after_stat(mean(tair_c))))+
  facet_grid(year~month)

d4$term %>% table


fn <- function(fname) names(fread(fname,nrows = 100))
tmp <- map(flist,possibly(fn,NULL))
tmp2 <- map(tmp, function(x) like(x,pattern="CO2") %>% sum)
tmp2 %>% unlist
tmp[[5]]
