pacman::p_load(tidyverse,data.table,mgcv,nls.multstart,arrow,furrr,lubridate)
source("src/R/functions_sif.R")


# Data prep ---------------------------------------------------------------
dat <- lapply(
  list.files("../data_general/proc_sif-optim/merged_parquets/",pattern = 'lc-41',full.names = T),
  arrow::read_parquet)
dat <- rbindlist(dat)
dat[,lc := 41]
dat[,`:=`(lst = lst*0.02 - 273.15, 
          sif = sif/1000)][,`:=`(xc=round(x*4)/4,
  yc=round(y*4)/4)][,`:=`(month=month(date),
    year=year(date))]
g <- unique(dat[,.(xc,yc)])[,.(id=.GRP),by=.(xc,yc)]
dat <- merge(dat,g,by=c("xc","yc"))



# furrr approach -----------------------------------------------
vec_ids <- unique(dat$id)
vec1 <- split(vec_ids, cut(seq_along(vec_ids), 4, labels = FALSE))[[1]]
vec2 <- split(vec_ids, cut(seq_along(vec_ids), 4, labels = FALSE))[[2]]
vec3 <- split(vec_ids, cut(seq_along(vec_ids), 4, labels = FALSE))[[3]]
vec4 <- split(vec_ids, cut(seq_along(vec_ids), 4, labels = FALSE))[[4]]


gc(full=TRUE)
plan(multisession, workers=20)
system.time(out1 <- mdat[id%in%vec1] %>% 
              split(.$id) %>%
              future_map(~fn_logistic_growth(.x),
                .progress = TRUE, 
                .options = furrr_options(seed=333L)) %>% 
              future_map_dfr(~ as_tibble(.), .id='id')
)



dnobs <- dat[,.(nobs = .N),by=.(xc,yc,year)]
dnobs %>% 
  ggplot(data=.,aes(xc,yc,fill=nobs))+
  geom_tile()+
  scale_fill_viridis_c(limits=c(0,500))+
  coord_equal()
  # facet_wrap(~cut(pdsi,c(-Inf,-0.1,0.1,Inf)),ncol=1)

mnobs <- dat[,.(nobs = sum(is.na(pdsi)==T)),by=.(xc,yc,year)]
mnobs$nobs %>% hist
mnobs %>% 
  ggplot(data=.,aes(xc,yc,fill=nobs))+
  geom_raster()+
  scale_fill_viridis_c(limits=c(0,20000),option='C',oob=scales::squish)+
  coord_equal()+
  facet_wrap(~year)


vec_ids <- sample(6724,1)
paf_dt(dat[id%in%vec_ids])
lm(sif~lst,data=dat[id%in%vec_ids]) %>% summary

dat[id%in%vec_ids][is.na(pdsi)==F] %>% 
  ggplot(data=.,aes(lst,sif,color=factor(id)))+
  geom_smooth(method='bam', 
    formula=y~s(x,bs='cs',k=5),
    method.args=list(select=T))+
  facet_wrap(~cut_interval(pdsi,3),ncol=1,scales='free_y')

dat[xc==-111.5 & yc==40] %>% 
  .[,`:=`(month=month(date),
    year=year(date))] %>% 
  .[month %in% c(5,6,7,8,9)] %>% 
  ggplot(data=.,aes(lst,sif))+
  # geom_point()+
  geom_smooth(se=F,
    method='bam',
    formula=y~s(x,bs='cs',k=5),
    method.args=list(select=T,discrete=T))+
  scale_color_viridis_c(option='B')+
  facet_wrap(~month,ncol=1,scales='free_y')



tmp_d <- dat[pdsi>4][sample(.N,10000)]
tmp_0 <- dat[pdsi==0][sample(.N,10000)]
tmp_w <- dat[pdsi< -4][sample(.N,10000)]


paf(dat[between(pdsi,4,5)][sample(.N,1000)]) %>% broom::tidy()

sel <- dat[sample(.N,1),c("x","y")]
tol <- 0.25
tmp <- dat[near(x,sel$x,tol = tol)][near(y,sel$y,tol=tol)]
unique(tmp[,.(x,y)])
tmp$pdsi %>% table
tmp %>% 
  ggplot(data=.,aes(lst,sif,color=cut_interval(pdsi,3)))+
  geom_point(alpha=0.01)+
  geom_smooth(method='gam',formula=y~s(x,k=5))+
  facet_wrap(~month)

paf(tmp) %>% broom::tidy()
paf(tmp[pdsi<0]) %>% broom::tidy()
paf(tmp[pdsi==0]) %>% broom::tidy()
paf(tmp[pdsi>0]) %>% broom::tidy()



paf(tmp_0)
paf(tmp_w)

dat$pdsi %>% hist

round(33.575*2)/2

dnobs[nobs==max(nobs)]

tmp <- dat[,`:=`(g = cut_interval(pdsi,30))][,.(p95 = quantile(sif,0.95,na.rm=T),
                                                pdsi = mean(pdsi)),by=g]
tmp %>% 
  ggplot(data=.,aes(pdsi,p95))+geom_point()
dat <- merge(dat,tmp[,.(g,p95)])
t2 <- dat[sif>p95][,.(v_sif = mean(sif,na.rm=T),
                v_lst = mean(lst,na.rm=T), 
                v_pdsi = mean(pdsi,na.rm=T)), by=g]
t2 %>% 
  ggplot(data=.,aes(v_pdsi,v_lst))+
  geom_point()


tmp$cut_interval %>% str_remove(.,"\\(") %>% str_remove(.,"\\]") %>% str_remove(.,"\\[")

tmp$cut_interval %>% str_remove(.,"\\(") %>% str_remove(.,"\\]") %>% str_remove(.,"\\[")

cut_interval(rnorm(100),3,labels=F)

dat[is.na(pdsi)==F][sample(.N,1000000)] %>% 
  ggplot(data=.,aes(lst,sif,color=cut_interval(pdsi,n=10)))+
  # geom_point()+
  geom_smooth(method='bam',
    formula=y~s(x),
    method.args=list(discrete=T))















dat <- arrow::read_parquet("../data_general/proc_sif-optim/merged_parquets/lc-41_sliver_sif_lst_1km_2020_.parquet")
dat[sample(.N,1e6)]$pdsi %>% summary
dat[sample(.N,1e6)]$sif %>% summary
dat[sample(.N,1e6)]$lst %>% summary


dat[sample(.N,1000000)] %>% 
  ggplot(data=.,aes(lst,sif,color=cut_interval(pdsi,n=3)))+
  # geom_point()+
  geom_smooth(method='bam',
    formula=y~s(x),
    method.args=list(discrete=T))
  # facet_wrap(~cut_number(pdsi,n=3),ncol = 1,scales='free_y')

dat[sample(.N,100000)] %>% 
  ggplot(data=.,aes(pdsi,lst))+
  # geom_point()+
  geom_smooth(method='lm')


s <- stars::read_stars("../data_general/proc_sif-optim/sif-subset-by-lc/MYD11A2_lc_42_2018-05-01_2018-09-30-0000000000-0000000000.tif",proxy=F)
names(s) <- 'sif'
val <- s[,,,1]$sif
val <- as.numeric(val)
summary(val[1:1000000]/100)

dat[date==ymd("2019-08-15")] %>% 
  ggplot(data=.,aes(x,y,fill=sif))+
  geom_raster()+
  scale_fill_viridis_c(option='B')+
  coord_equal()

dat %>% dim


roi
s <- st_crop(s,st_bbox(c(xmin=-124.7401,xmax=xs[3],ymax=49,ymin=25),crs=st_crs(4326)))
ggplot()+
  geom_stars(data=m[,,,1])+
  geom_sf(data=st_as_sfc(roi),col='red',fill=NA)+
  coord_sf()




l7 = read_stars(system.file("tif/L7_ETMs.tif", package = "stars"))
d = st_dimensions(l7)

# area around cells 3:10 (x) and 4:11 (y):
offset = c(d[["x"]]$offset, d[["y"]]$offset)
res = c(d[["x"]]$delta, d[["y"]]$delta)
bb = st_bbox(c(xmin = offset[1] + 2 * res[1],
	ymin = offset[2] + 11 * res[2],
	xmax = offset[1] + 10 * res[1],
	ymax = offset[2] +  3 * res[2]), crs = st_crs(l7))
l7[bb]

bb
st_bbox(l7)
st_bbox(l7[bb])


st_bbox(m)
roi
m1 <- st_crop(m,roi,crop=T,collect=T,epsilon = 0.999)
st_bbox(m1)

plot(m1[,,,1],breaks='equal')


ggplot()+
  geom_stars(data=m[,,,1])+
  geom_sf(data=st_as_sfc(roi),col='red',fill=NA)+
  coord_sf()


dat[date==ymd("2018-07-01")] %>% 
  ggplot(data=.,aes(x,y,fill=lst*0.02 - 273.15))+
  geom_tile()+
  scale_fill_viridis_c()+
  coord_equal()

