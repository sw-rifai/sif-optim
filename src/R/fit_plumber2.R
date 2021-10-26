pacman::p_load(tidyverse,data.table,stars,lubridate, furrr)

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
  out1 <- din[SWdown>80][
    is.na(GPP)==F
  ][
    is.na(TairC)==F
  ][
    month%in%c(5:9)
  ][,`:=`(TaC = round(TairC*2)/2)][
    ,.(gpp_lai90 = quantile(GPP/LAI,0.9),
       gpp90 = quantile(GPP,0.9)),
    by=.(site,year,month,TaC)
  ]
  out2 <- din[SWdown>80][
    is.na(GPP)==F
  ][
    is.na(TairC)==F
  ][
    month%in%c(5:9)
  ][,`:=`(TaC = round(TairC*2)/2)][
    ,.(tmin = min(TairC),
      tmax = max(TairC),
      tmean = mean(TairC)),by=.(site,year,month)
  ]
  out <- merge(out1,out2,by=c("site","year","month"))
  return(out)
}

d1 <- lapply(enf,fn_prep)
d2 <- lapply(d1,fn_quant)
d3 <- rbindlist(d2)

plan(multisession,workers=5)
d4 <- d3 %>%
        split(f=list(.$site,.$year,.$month),
          drop=T) %>%
        future_map(~parab_dt_monthly(.x)) %>%
    rbindlist()

d5 <- merge(d4,unique(d3[,.(site,year,month,tmin,tmax,tmean)]),
  all.x=T,
  all.y=F,
  by=c("site","year","month"), allow.cartesian = T)
arrow::write_parquet(d5,"../data_general/proc_sif-optim/mod_fits/plumber2/enf_topt.parquet")
d5 <- arrow::read_parquet("../data_general/proc_sif-optim/mod_fits/plumber2/enf_topt.parquet")
d5[term=='rsq']

d5[term=='Topt'][p.value<0.05][std.error < 5] %>% 
  ggplot(data=.,aes(tmean,estimate,color=site))+
  geom_point()+
  geom_smooth(method='lm', se=F, inherit.aes = F,
    aes(tmean,estimate),color='grey40',lwd=2)+
  geom_smooth(method='lm', se=F)+
  scico::scale_color_scico_d(palette = 'batlow',end=0.8,begin=0.1)+
  labs(x='Mean Air Temp. during peak SW (°C)',
    y=expression(paste(T[opt],"(°C)")),
    title='Evergreen Needle Forests',
    color='Site')+
  theme_linedraw()+
  theme(panel.grid = element_blank())
ggsave(
  filename = "figures/prelim_enf_Topt_plumber2.png",
  width=20,
  height=15,
  units='cm',
  dpi=300)


d5[term%in%c("Topt","p_opt","rsq")][p.value<0.05][std.error<5] %>% 
  pivot_wider(id_cols = c(year,site,month), 
    names_from='term',values_from = 'estimate') %>% 
  ggplot(data=.,aes(Topt,p_opt,color=site))+
  geom_point()+
  geom_smooth(method='lm', se=F, inherit.aes = F,
    aes(Topt,p_opt),color='grey40',lwd=2)+
  geom_smooth(method='lm', se=F)+
  scico::scale_color_scico_d(palette = 'batlow',end=0.8,begin=0.1)+
  labs(
    y=expression(paste(P[opt]~"(µmol"~m**-2~s**-1,")")),
    x=expression(paste(T[opt],"(°C)")),
    title='Evergreen Needle Forests',
    color='Site')+
  theme_linedraw()+
  theme(panel.grid = element_blank())
ggsave(
  filename = "figures/prelim_enf_Topt-Popt_plumber2.png",
  width=20,
  height=15,
  units='cm',
  dpi=300)

d5[term=='Topt'][p.value<0.05][std.error < 5] %>% 
  ggplot(data=.,aes(tmean,estimate,color=site))+
  geom_segment(aes(x=tmin,xend=tmax,y=estimate,yend=estimate,group=site),
    color='grey80')+
    geom_point()+
  geom_smooth(method='lm', se=F, inherit.aes = F,
    aes(tmean,estimate),color='grey40',lwd=2)+
  geom_smooth(method='lm', se=F)+
  scico::scale_color_scico_d(palette = 'batlow',end=0.8,begin=0.1)+
  labs(x='Mean Air Temp. during peak SW (°C)',
    y=expression(paste(T[opt],"(°C)")),
    title='Evergreen Needleleaf Forests',
    color='Site')+
  theme_linedraw()+
  theme(panel.grid = element_blank())
ggsave(
  filename = "figures/prelim_enf_Topt_Trange_plumber2.png",
  width=20,
  height=15,
  units='cm',
  dpi=300)



# DBF ---------------------------------------------------------------------

d1 <- lapply(dbf,fn_prep)
d2 <- lapply(d1,fn_quant)
d3 <- rbindlist(d2)

plan(multisession,workers=5)
d4 <- d3 %>%
        split(f=list(.$site,.$year,.$month),
          drop=T) %>%
        future_map(~parab_dt_monthly(.x)) %>%
    rbindlist()
d4[term=='Topt']
d4[term=='rsq']

d5 <- merge(d4,unique(d3[,.(site,year,month,tmin,tmax,tmean)]),
  all.x=T,
  all.y=F,
  by=c("site","year","month"), allow.cartesian = T)
arrow::write_parquet(d5,"../data_general/proc_sif-optim/mod_fits/plumber2/dbf_topt.parquet")
d5[term=='rsq']

d5[site=='US-WCr'][term=='Topt'][estimate>=35]
d5[site=='US-WCr'][year==2002 & month==5]

d5[term=='Topt'][p.value<0.05][std.error < 5] %>% 
  ggplot(data=.,aes(tmean,estimate,color=site))+
  geom_point()+
  geom_smooth(method='lm', se=F, inherit.aes = F,
    aes(tmean,estimate),color='grey40',lwd=2)+
  geom_smooth(method='lm', se=F)+
  scico::scale_color_scico_d(palette = 'batlow',end=0.8,begin=0.1)+
  labs(x='Mean Air Temp. during peak SW (°C)',
    y=expression(paste(T[opt],"(°C)")),
    title='Decidous Broadleaf Forests',
    color='Site')+
  theme_linedraw()+
  theme(panel.grid = element_blank())
ggsave(
  filename = "figures/prelim_dbf_Topt_plumber2.png",
  width=20,
  height=15,
  units='cm',
  dpi=300)


d5[term%in%c("Topt","p_opt","rsq")][p.value<0.05][std.error<5] %>% 
  pivot_wider(id_cols = c(year,site,month), 
    names_from='term',values_from = 'estimate') %>% 
  ggplot(data=.,aes(Topt,p_opt,color=site))+
  geom_point()+
  geom_smooth(method='lm', se=F, inherit.aes = F,
    aes(Topt,p_opt),color='grey40',lwd=2)+
  geom_smooth(method='lm', se=F)+
  scico::scale_color_scico_d(palette = 'batlow',end=0.8,begin=0.1)+
  labs(
    y=expression(paste(P[opt]~"(µmol"~m**-2~s**-1,")")),
    x=expression(paste(T[opt],"(°C)")),
    title='Deciduous Broadleaf Forests',
    color='Site')+
  theme_linedraw()+
  theme(panel.grid = element_blank())
ggsave(
  filename = "figures/prelim_dbf_Topt-Popt_plumber2.png",
  width=20,
  height=15,
  units='cm',
  dpi=300)


d5[term=='Topt'][p.value<0.05][std.error < 5] %>% 
  ggplot(data=.,aes(tmean,estimate,color=site))+
    geom_abline(col='#cf0000',lty=1,lwd=1)+
    annotate('text',x=-5,y=32,color='black',
      label='1:1 line')+
    annotate('rect',xmin=-10,xmax=0,ymin=30,ymax=30,color='#cf0000')+
  geom_segment(aes(x=tmin,xend=tmax,y=estimate,yend=estimate,group=site),
    color='grey80')+
    geom_point()+
  geom_smooth(method='lm', se=F, inherit.aes = F,
    aes(tmean,estimate),color='grey40',lwd=2)+
  geom_smooth(method='lm', se=F)+
  scico::scale_color_scico_d(palette = 'batlow',end=0.8,begin=0.1)+
  labs(x='Mean Air Temp. during peak SW (°C)',
    y=expression(paste(T[opt],"(°C)")),
    title='Decidous Broadleaf Forests',
    color='Site')+
  theme_linedraw()+
  theme(panel.grid = element_blank())
ggsave(
  filename = "figures/prelim_dbf_Topt_Trange_plumber2.png",
  width=20,
  height=15,
  units='cm',
  dpi=300)



d5[term=='Topt'][p.value<0.05][std.error < 5] %>% 
  ggplot(data=.,aes(tmean,estimate,color=site))+
  geom_abline(col='#cf0000',lty=1,lwd=1)+
  geom_segment(aes(x=tmin,xend=tmax,y=estimate,yend=estimate,group=site),
    color='grey80')+
    geom_point()+
  geom_smooth(method='lm', se=F, inherit.aes = F,
    aes(tmean,estimate),color='grey40',lwd=2)+
  # geom_smooth(method='lm', se=F)+
  scico::scale_color_scico_d(palette = 'batlow',end=0.8,begin=0.1)+
  labs(x='Mean Air Temp. during peak SW (°C)',
    y=expression(paste(T[opt],"(°C)")),
    title='Decidous Broadleaf Forests',
    color='Site')+
  theme_linedraw()+
  theme(panel.grid = element_blank())
