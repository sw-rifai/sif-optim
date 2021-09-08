pacman::p_load(tidyverse,data.table,mgcv,nls.multstart,arrow,furrr,lubridate)
future::availableCores()
future::availableWorkers()
setwd("/home/sami/srifai@gmail.com/work/research/sif-optim/")
print(getwd())
source("src/R/functions_sif.R")

# options -----------------------------------------------------------------
lc_class <- 90
min_nobs <- 30
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


# furrr approach -----------------------------------------------
vec_ids <- unique(dat$id)
l_ids <- split(vec_ids, cut(seq_along(vec_ids), n_splits, labels = FALSE))

plan(cluster, workers=n_cores)
all_out <- list()
for(i in start_split_idx:n_splits){
  print(paste("starting split ",i));
  gc(full=T)
  system.time(all_out[[i]] <- dat[id%in%l_ids[[i]]] %>%
                split(f=list(.$id,.$dc),
                  drop=T) %>%
                future_map(~paf_dt(.x),
                  .progress = F,
                  .options = furrr_options(seed=333L)) %>%
    rbindlist(idcol = 'dc') %>%
    .[,`:=`(dc = gsub('[0-9]+\\.','', dc))]
)

    print(paste("writing split ",i));
  arrow::write_parquet(all_out[[i]], sink=paste0("../data_general/proc_sif-optim/mod_fits/",
  "lc-",lc_class,"-fits_split-",i,"_",Sys.Date(),".parquet"),
  compression = 'snappy')
  
}

gc(full=T)
all_out <- rbindlist(all_out)
gc(full=T)
arrow::write_parquet(all_out, sink=paste0("../data_general/proc_sif-optim/mod_fits/",
  "lc-",lc_class,"-fits_",Sys.Date(),".parquet"),
  compression = 'snappy')


# print("split 1");gc(full=T)
# system.time(out1 <- dat[id%in%l_ids[[1]]] %>% 
#               split(f=list(.$id,.$dc),
#                 drop=T) %>%
#               future_map(~paf_dt(.x),
#                 .progress = TRUE, 
#                 .options = furrr_options(seed=333L)) %>% 
#   rbindlist(idcol = 'dc') %>% 
#   .[,`:=`(dc = gsub('[0-9]+\\.','', dc))]
# )
# print("split 2");gc(full=T)
# system.time(out2 <- dat[id%in%l_ids[[2]]] %>% 
#               split(f=list(.$id,.$dc),
#                 drop=T) %>%
#               future_map(~paf_dt(.x),
#                 .progress = TRUE, 
#                 .options = furrr_options(seed=333L)) %>% 
#   rbindlist(idcol = 'dc') %>% 
#   .[,`:=`(dc = gsub('[0-9]+\\.','', dc))]
# )
# print("split 3");gc(full=T)
# system.time(out3 <- dat[id%in%l_ids[[3]]] %>% 
#               split(f=list(.$id,.$dc),
#                 drop=T) %>%
#               future_map(~paf_dt(.x),
#                 .progress = TRUE, 
#                 .options = furrr_options(seed=333L)) %>% 
#   rbindlist(idcol = 'dc') %>% 
#   .[,`:=`(dc = gsub('[0-9]+\\.','', dc))]
# )
# 
# print("split 4");gc(full=T)
# system.time(out4 <- dat[id%in%l_ids[[4]]] %>% 
#               split(f=list(.$id,.$dc),
#                 drop=T) %>%
#               future_map(~paf_dt(.x),
#                 .progress = TRUE, 
#                 .options = furrr_options(seed=333L)) %>% 
#   rbindlist(idcol = 'dc') %>% 
#   .[,`:=`(dc = gsub('[0-9]+\\.','', dc))]
# )
# 
# print("split 5");gc(full=T)
# system.time(out5 <- dat[id%in%l_ids[[5]]] %>% 
#               split(f=list(.$id,.$dc),
#                 drop=T) %>%
#               future_map(~paf_dt(.x),
#                 .progress = TRUE, 
#                 .options = furrr_options(seed=333L)) %>% 
#   rbindlist(idcol = 'dc') %>% 
#   .[,`:=`(dc = gsub('[0-9]+\\.','', dc))]
# )
# 
# print("split 6");gc(full=T)
# system.time(out6 <- dat[id%in%l_ids[[6]]] %>% 
#               split(f=list(.$id,.$dc),
#                 drop=T) %>%
#               future_map(~paf_dt(.x),
#                 .progress = TRUE, 
#                 .options = furrr_options(seed=333L)) %>% 
#   rbindlist(idcol = 'dc') %>% 
#   .[,`:=`(dc = gsub('[0-9]+\\.','', dc))]
# )
# 
# print("split 7");gc(full=T)
# system.time(out7 <- dat[id%in%l_ids[[7]]] %>% 
#               split(f=list(.$id,.$dc),
#                 drop=T) %>%
#               future_map(~paf_dt(.x),
#                 .progress = TRUE, 
#                 .options = furrr_options(seed=333L)) %>% 
#   rbindlist(idcol = 'dc') %>% 
#   .[,`:=`(dc = gsub('[0-9]+\\.','', dc))]
# )
# 
# print("split 8");gc(full=T)
# system.time(out8 <- dat[id%in%l_ids[[8]]] %>% 
#               split(f=list(.$id,.$dc),
#                 drop=T) %>%
#               future_map(~paf_dt(.x),
#                 .progress = TRUE, 
#                 .options = furrr_options(seed=333L)) %>% 
#   rbindlist(idcol = 'dc') %>% 
#   .[,`:=`(dc = gsub('[0-9]+\\.','', dc))]
# )
# 
# print("split 9");gc(full=T)
# system.time(out9 <- dat[id%in%l_ids[[9]]] %>% 
#               split(f=list(.$id,.$dc),
#                 drop=T) %>%
#               future_map(~paf_dt(.x),
#                 .progress = TRUE, 
#                 .options = furrr_options(seed=333L)) %>% 
#   rbindlist(idcol = 'dc') %>% 
#   .[,`:=`(dc = gsub('[0-9]+\\.','', dc))]
# )
# 
# print("split 10");gc(full=T)
# system.time(out10 <- dat[id%in%l_ids[[10]]] %>% 
#               split(f=list(.$id,.$dc),
#                 drop=T) %>%
#               future_map(~paf_dt(.x),
#                 .progress = TRUE, 
#                 .options = furrr_options(seed=333L)) %>% 
#   rbindlist(idcol = 'dc') %>% 
#   .[,`:=`(dc = gsub('[0-9]+\\.','', dc))]
# )
# gc(full=T)

# all_out <- rbindlist(list(out1,out2,out3,out4,out5,out6,out7,out8, 
#   out9,out10))
# gc(full=T)
# arrow::write_parquet(all_out, sink=paste0("../data_general/proc_sif-optim/mod_fits/",
#   "lc-",lc_class,"-fits_",Sys.Date(),".parquet"),
#   compression = 'snappy')
