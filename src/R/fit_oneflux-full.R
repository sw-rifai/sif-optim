pacman::p_load(tidyverse,data.table,stars,lubridate, furrr)
source("src/R/functions_sif.R")

# fluxnet15 ----------------------------------------------------------------
flist <- list.files("/home/sami/srifai@gmail.com/work/research/data_general/fluxnet/oneflux/", 
  recursive = T,full.names = T,pattern = "_HH_")

str_extract(flist,"FLX\\_US\\-\\b[a-zA-Z0-9]{3}") # THIS IS THE REGEX KEY!

fn_prep_oneflux <- function(fname){
  raw <- fread(fname)
  raw[,`:=`(date = ymd_hm(TIMESTAMP_START))]
  raw[,`:=`(year=year(date),
    month=month(date),
    hour=hour(date))]
  raw <- raw[month%in%5:9]
  
  if("SW_IN_F_MDS"%in%names(raw)){
    sw_q <- raw[,.(sw80 = quantile(SW_IN_F_MDS,0.8,na.rm=T)),by=month]
    raw <- merge(raw,sw_q)[SW_IN_F_MDS>sw80]
  }else{
    sw_q <- raw[,.(sw80 = quantile(SW_IN_ERA,0.8,na.rm=T)),by=month]
    raw <- merge(raw,sw_q)[SW_IN_ERA>sw80]
  }
  # raw <- merge(raw,sw_q)[SW_IN_F_MDS>sw80]
  # 
  if("TA_F_MDS"%in%names(raw)){
  raw <- raw[TA_F_MDS > -99]
  raw[,TaC := round(TA_F_MDS*4)/4]
  }else{
  raw <- raw[TA_ERA > -99]
  raw[,TaC := round(TA_ERA*4)/4]
  }
  q_ta <- raw[
    ,.(tmin = min(TaC),
      tmax = max(TaC),
      tmean = mean(TaC)),by=.(year,month)
  ]
 if("GPP_NT_CUT_REF" %in% names(raw)){
  o <- raw[,.(gpp90 = quantile(GPP_NT_CUT_REF,0.90)),by=.(year,month,TaC)]
 }else{
  o <- raw[,.(gpp90 = quantile(GPP_DT_VUT_REF,0.90)),by=.(year,month,TaC)]
 }
 site <- str_extract(fname,"FLX\\_US\\-\\b[a-zA-Z0-9]{3}")
 o$site <- site
 o <- merge(o,q_ta,by=c("year","month"))
 return(o)
}

fn_prep_oneflux(flist[1])
# scratch, delete
# for(i in 1:length(flist)){
#   print(i)
#   out <- tryCatch(fn_prep_oneflux(flist[i]), error=function(e) NULL)
#   print(out)
# }

plan(multisession,workers=5)
d1 <- future_map(flist, possibly(fn_prep_oneflux,NA)) # map pre-processing function
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

gm <- fread("../data_general/proc_sif-optim/fluxnet_ee_exports/gridmet_pdsi_fluxnet15_1995_2021.csv")
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
    title='Oneflux-Ameriflux2015 US sites',
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

fn_prep_oneflux(flist[6]) %>% 
  ggplot(data=.,aes(TaC,gpp90))+
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
