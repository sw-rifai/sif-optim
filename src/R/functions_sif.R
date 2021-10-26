
# Functions ---------------------------------------------------------------
paf <- function(din,iter=10) nls.multstart::nls_multstart(sif ~ kopt * ((Hd * (2.718282^((Ha*(lst-Topt))/(lst*0.008314*Topt)))) / 
                                (Hd - (Ha*(1-(2.718282^((Hd*(lst-Topt))/(lst*0.008314*Topt))))))),
                         data = din,
                         iter = iter,
                         start_lower = c(kopt = 2, Hd = 200, Ha = 50, Topt = 15),
                         start_upper = c(kopt = 5, Hd = 200, Ha = 300, Topt = 35),
                         supp_errors = 'Y',
                         na.action = na.omit,
                         #convergence_count = 500,
                         lower = c(kopt = 0.1, Hd = 0, Ha = 0, Topt = 0))


paf_dt <- function(din,iter=10,force_pa=F){
   fit_lm <- lm(sif~lst,data=din)
   fit_pa <- nls.multstart::nls_multstart(sif ~ kopt * ((Hd * (2.718282^((Ha*(lst-Topt))/(lst*0.008314*Topt)))) / 
                                  (Hd - (Ha*(1-(2.718282^((Hd*(lst-Topt))/(lst*0.008314*Topt))))))),
                           data = din,
                           iter = iter,
                           start_lower = c(kopt = 1, Hd = 200, Ha = 50, Topt = 15),
                           start_upper = c(kopt = 4, Hd = 200, Ha = 300, Topt = 35),
                           supp_errors = 'Y',
                           na.action = na.omit,
                           #convergence_count = 500,
                           lower = c(kopt = 0.1, Hd = 0, Ha = 0, Topt = 0),
                           upper = c(kopt = 20, Hd = 1000,Ha=1000,Topt=50))
   if(is.null(fit_pa)==F & is.null(fit_lm)==F){
      best_fit <- attr(bbmle::AICtab(fit_pa,fit_lm),"row.names")[1]
      out_fit <- eval(as.symbol(best_fit))
      if(force_pa==T){out_fit <- fit_pa}
   }else{
         out_fit <- fit_lm
      }
   out <- out_fit %>% broom::tidy(.,conf.int=F)
   rsq <- tibble(term="rsq",
                     estimate=yardstick::rsq_trad_vec(din$sif, predict(out_fit)))
   mae <- tibble(term="mae",
                     estimate=yardstick::mae_vec(din$sif, predict(out_fit)))
   nobs <- tibble(term='nobs',
      estimate=dim(din)[1])
   out <- bind_rows(out,rsq,mae,nobs)
   out$xc <- unique(din$xc)[1]
   out$yc <- unique(din$yc)[1]
   out$lc <- unique(din$lc)
   id <- unique(din$id)
   if(is.null(id)==T){
     out$id <- NA_integer_
     }else{out$id <- id}
   gc(full=T)
   return(out)
}

paf_dt_monthly <- function(din,iter=10,force_pa=F){
   fit_lm <- lm(sif~lst,data=din)
   fit_pa <- nls.multstart::nls_multstart(sif ~ kopt * ((Hd * (2.718282^((Ha*(lst-Topt))/(lst*0.008314*Topt)))) / 
                                  (Hd - (Ha*(1-(2.718282^((Hd*(lst-Topt))/(lst*0.008314*Topt))))))),
                           data = din,
                           iter = iter,
                           start_lower = c(kopt = 1, Hd = 200, Ha = 50, Topt = 15),
                           start_upper = c(kopt = 4, Hd = 200, Ha = 300, Topt = 35),
                           supp_errors = 'Y',
                           na.action = na.omit,
                           #convergence_count = 500,
                           lower = c(kopt = 0.1, Hd = 0, Ha = 0, Topt = 0),
                           upper = c(kopt = 20, Hd = 1000,Ha=1000,Topt=50))
   if(is.null(fit_pa)==F & is.null(fit_lm)==F){
      best_fit <- attr(bbmle::AICtab(fit_pa,fit_lm),"row.names")[1]
      out_fit <- eval(as.symbol(best_fit))
      if(force_pa==T){out_fit <- fit_pa}
   }else{
         out_fit <- fit_lm
      }
   out <- out_fit %>% broom::tidy(.,conf.int=F)
   rsq <- tibble(term="rsq",
                     estimate=yardstick::rsq_trad_vec(din$sif, predict(out_fit)))
   mae <- tibble(term="mae",
                     estimate=yardstick::mae_vec(din$sif, predict(out_fit)))
   nobs <- tibble(term='nobs',
      estimate=dim(din)[1])
   out <- bind_rows(out,rsq,mae,nobs)
   out$xc <- unique(din$xc)[1]
   out$yc <- unique(din$yc)[1]
   out$lc <- unique(din$lc)
   id <- unique(din$id)
   if(is.null(id)==T){
     out$id <- NA_integer_
   }else{out$id <- id}
   out$year <- unique(din$year)[1]
   out$month <- unique(din$month)[1]
   gc(full=T)
   return(out)
}


parab_dt_monthly <- function(din,iter=10,force_pa=F){
   if("response90"%in%names(din)){
      din[,`:=`(gpp90=response90)]
   }
   fit_lm <- lm(gpp90~TaC,data=din)
   fit_pa <- nls.multstart::nls_multstart(gpp90 ~ p_opt-b*(TaC-Topt)**2,
                           data = din,
                           iter = iter,
                           start_lower = c(p_opt=5,b=1,Topt = 15),
                           start_upper = c(p_opt=10,b=1,Topt = 35),
                           supp_errors = 'Y',
                           na.action = na.omit,
                           #convergence_count = 500,
                           lower = c(p_opt=0, b=0.001, Topt = 0),
                           upper = c(p_opt=100, b=10, Topt=39))
   if(is.null(fit_pa)==F & is.null(fit_lm)==F){
      best_fit <- attr(bbmle::AICtab(fit_pa,fit_lm),"row.names")[1]
      out_fit <- eval(as.symbol(best_fit))
      if(force_pa==T){out_fit <- fit_pa}
   }else{
         out_fit <- fit_lm
      }
   out <- out_fit %>% broom::tidy(.,conf.int=F)
   rsq <- tibble(term="rsq",
                     estimate=yardstick::rsq_trad_vec(din$gpp90, predict(out_fit)))
   mae <- tibble(term="mae",
                     estimate=yardstick::mae_vec(din$gpp90, predict(out_fit)))
   nobs <- tibble(term='nobs',
      estimate=dim(din)[1])
   # nobs <- dim(din)[1]
   out <- bind_rows(out,rsq,mae,nobs)
   # out$xc <- unique(din$xc)[1]
   # out$yc <- unique(din$yc)[1]
   # out$lc <- unique(din$lc)
   # id <- unique(din$id)
   # if(is.null(id)==T){
   #   out$id <- NA_integer_
   # }else{out$id <- id}
   out$year <- unique(din$year)[1]
   out$month <- unique(din$month)[1]
   out$site <- unique(din$site)
   # out$nobs <- nobs$estimate
   gc(full=T)
   return(out)
}


paf_dt_3week <- function(din,iter=10,force_pa=F,min_nobs=10){
   if(nrow(din)<min_nobs){return(NULL)}
   fit_lm <- lm(sif~lst,data=din)
   fit_pa <- nls.multstart::nls_multstart(sif ~ kopt * ((Hd * (2.718282^((Ha*(lst-Topt))/(lst*0.008314*Topt)))) / 
                                  (Hd - (Ha*(1-(2.718282^((Hd*(lst-Topt))/(lst*0.008314*Topt))))))),
                           data = din,
                           iter = iter,
                           start_lower = c(kopt = 1, Hd = 200, Ha = 50, Topt = 15),
                           start_upper = c(kopt = 4, Hd = 200, Ha = 300, Topt = 35),
                           supp_errors = 'Y',
                           na.action = na.omit,
                           #convergence_count = 500,
                           lower = c(kopt = 0.1, Hd = 0, Ha = 0, Topt = 0),
                           upper = c(kopt = 20, Hd = 1000,Ha=1000,Topt=50))
   if(is.null(fit_pa)==F & is.null(fit_lm)==F){
      best_fit <- attr(bbmle::AICtab(fit_pa,fit_lm),"row.names")[1]
      out_fit <- eval(as.symbol(best_fit))
      if(force_pa==T){out_fit <- fit_pa}
   }else{
         out_fit <- fit_lm
      }
   out <- out_fit %>% broom::tidy(.,conf.int=F)
   rsq <- tibble(term="rsq",
                     estimate=yardstick::rsq_trad_vec(din$sif, predict(out_fit)))
   mae <- tibble(term="mae",
                     estimate=yardstick::mae_vec(din$sif, predict(out_fit)))
   nobs <- tibble(term='nobs',
      estimate=dim(din)[1])
   out <- bind_rows(out,rsq,mae,nobs)
   out$xc <- unique(din$xc)[1]
   out$yc <- unique(din$yc)[1]
   out$lc <- unique(din$lc)
   id <- unique(din$id)
   if(is.null(id)==T){
     out$id <- NA_integer_
   }else{out$id <- id}
   out$year <- unique(din$year)[1]
   out$month <- unique(din$month)[1]
   out$week_center <- unique(din$week)[1]
   gc(full=T)
   return(out)
}


# q <- dat %>% 
#   filter(SWdown > sw80) %>% 
#   # sample_n(10000) %>% 
#   filter(is.na(GPP)==F) %>% 
#   filter(is.na(TairC)==F) %>% 
#   filter(month%in%c(5:9)) %>% 
#   mutate(TaC = round(TairC*2)/2) %>% #pull(TaC)
#   group_by(year,month,TaC) %>%
#   summarize(gpp = quantile(GPP/LAI,
#     0.9,
#     na.rm=T), 
#      tmin = min(TairC),
#      tmax = max(TairC),
#      tmean = mean(TairC)) %>% 
#   ungroup()
# 
# q2 <- dat %>% 
#   filter(SWdown > sw80) %>% 
#   # sample_n(10000) %>% 
#   filter(is.na(GPP)==F) %>% 
#   filter(is.na(TairC)==F) %>% 
#   filter(month%in%c(5:9)) %>% 
#   mutate(TaC = round(TairC*2)/2) %>% #pull(TaC)
#   group_by(year,month) %>%
#   summarize(gpp = quantile(GPP/LAI,
#     0.9,
#     na.rm=T), 
#      tmin = min(TairC),
#      tmax = max(TairC),
#      tmean = mean(TairC)) %>% 
#   ungroup()

# parab_dt_monthly(q)
# vec_ids <- sample(6724,1)
# din <- dat[id%in%vec_ids]
# paf_dt(dat[id%in%vec_ids])



# paf_dt(din,iter=100)
# paf_dt(dat[id==6000])
# din <- dat[id==6724]
# ?broom::augment
