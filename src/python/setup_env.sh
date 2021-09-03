conda create -n sci3 python=3.7
conda activate sci3
conda install -c conda-forge xesmf esmpy=7.1.0
conda install -c conda-forge dask netCDF4 matplotlib cartopy jupyterlab rasterio pandas nodejs