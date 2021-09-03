
data_dir="/home/sami/srifai@gmail.com/work/research/data_general/proc_sif-optim"
# ls $data_dir

gdal_translate -of netCDF -co "FOMRAT=NC4" $data_dir/myd11a1_2018-0000000000-0000000000.tif $data_dir/lst_scratch/myd11a1_2018-0000000000-0000000000.nc


