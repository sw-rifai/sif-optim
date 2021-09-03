cdo -f nc4c -z zip_6 cat /media/sami/srifai-ssd/data-ssd/tropomi-conus/TROPOMI_201802* test.nc

cdo -f nc4c -z zip_6 remapbil,sif_refgrid.nc myd11a1_2018-0000000000-0000000000.nc test.nc

ncremap -d sif_refgrid.nc myd11a1_2018-0000000000-0000000000.nc test.nc

ncremap -d sif_refgrid.nc ../smap_ssma_2018.tif test.nc

cdo -f nc4c -z zip_6 remapbil,test_grid.nc myd11a1_2018-0000000000-0000000000.nc test_lst.nc
