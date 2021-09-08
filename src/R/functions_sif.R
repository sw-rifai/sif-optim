
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



# vec_ids <- sample(6724,1)
# din <- dat[id%in%vec_ids]
# paf_dt(dat[id%in%vec_ids])



# paf_dt(din,iter=100)
# paf_dt(dat[id==6000])
# din <- dat[id==6724]
# ?broom::augment
