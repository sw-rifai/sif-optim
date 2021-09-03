import os
import re
import urllib.request

import cartopy.crs as ccrs
import matplotlib.pyplot as plt
import numpy as np
from rasterio.warp import transform
import pandas as pd
import xarray as xr
from dask.diagnostics import ProgressBar

import fnmatch
flist = []
for file_name in os.listdir("../../../data_general/proc_sif-optim/"):
    if fnmatch.fnmatch(file_name, 'myd11a1_*-*'):
#         print(file_name)
        flist.append(file_name)
    
import re
y = re.search("(\\d{4})",flist[0])
y = y.group()
vec_time = pd.date_range(y+"-01-01", periods=364)

def regrid_lst(fname):
    fpath = "../../../data_general/proc_sif-optim/"+fname
    fout_name = fname.rsplit('.')[0]+"_regrid.nc"
    fout_path = "../../../data_general/proc_sif-optim/covar_nc/"+fout_name
    ref = xr.open_dataset("../../../data_general/proc_sif-optim/covar_nc/sif_refgrid.nc")
    ref = ref['SIF']
    ref = ref.rename({"lon":"x",
           "lat":"y"})
    
    # get the year from the filename 
    y = re.search("(\\d{4})",flist[0])
    y = y.group()
    
    # open w/chunking
    da = xr.open_rasterio(fpath,chunks={"band":40})
    da = da.rename({'band':'time'})
    vec_time = pd.date_range(y+"-01-01", periods=len(da['time']))
    da.coords['time'] = vec_time
    
    # subset reference grid
    ref = ref.sel(x=slice(np.min(da['x']), np.max(da['x'])))
    ref = ref.sel(y=slice(np.min(da['y']), np.max(da['y'])))
    
    result = da.interp(x=ref['x'], y=ref['y'],method='nearest')
    with ProgressBar():
        result = result.compute()
        
    result.rename('lst').to_netcdf(fout_path,
                               format='netcdf4',
                              encoding = {'lst': {'dtype': 'int16', 
                                                  'scale_factor': 0.1,
                                                  'zlib':True}})
    return result

done_list = []
for file_name in os.listdir("../../../data_general/proc_sif-optim/covar_nc/"):
    if fnmatch.fnmatch(file_name, 'myd11a1_*-*'):
#         print(file_name)
        done_list.append(file_name.rsplit("_regrid")[0])
done_list

name_list = []
for file_name in os.listdir("../../../data_general/proc_sif-optim/"):
    if fnmatch.fnmatch(file_name, 'myd11a1_*-*'):
#         print(file_name)
        name_list.append(file_name.rsplit(".tif")[0])
name_list

to_do = set(name_list).symmetric_difference(done_list)
to_do = list(to_do)
to_do

for i in to_do:
    regrid_lst(i+".tif")