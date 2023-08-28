import xarray as xr
import numpy as np
import pandas as pd
from tqdm import tqdm
# function to load solar irradiance data

def load_solar_irradiance(lat, lon, time, var_names = ['daily_integral_of_surface_global_irradiance'],
                          grid = 'nearest',product_baseurl = 'https://dapds00.nci.org.au/thredds/dodsC/rv74/satellite-products/arc/der/himawari-ahi/solar/p1d/latest'):
    '''
    Retrive solar radiation products from THREDDS server
    Parameters:
        lat: tuple, latitude range
        lon: tuple, longitude range
        var_names: list of strings, mesurements/observations
        grid: string, option to snap to nearest grid of the data
        product_baseurl: string, base url of the THREDDS directory
    '''
    # lat, lon grid
    if grid == 'nearest':
        # select lat/lon range from data; snap to nearest grid
        lat_range, lon_range = None, None
    else:
        # define a grid that covers the entire area of interest
        lat_range = np.arange(np.max(np.ceil(np.array(lat)*10.+0.5)/10.-0.05), np.min(np.floor(np.array(lat)*10.-0.5)/10.+0.05)-0.05, -0.1)
        lon_range = np.arange(np.min(np.floor(np.array(lon)*10.-0.5)/10.+0.05), np.max(np.ceil(np.array(lon)*10.+0.5)/10.-0.05)+0.05, 0.1)
    daterange = pd.date_range(time[0],time[1])
    datasets = []
    for single_date in tqdm(daterange):
        # retrive year, month and day for directory construction
        year=str(single_date.year)
        month=str(single_date.month).zfill(2)
        day=str(single_date.day).zfill(2)
        product_url='/'.join([product_baseurl,year,month,day])+'/IDE02326.'+year+month+day+'0000.nc'
        # print(product_url)
        
        # data is loaded lazily through OPeNDAP
        ds = xr.open_dataset(product_url)
        if lat_range is None:
            # select lat/lon range from data if not specified; snap to nearest grid
            test = ds.sel(latitude=list(lat), longitude=list(lon), method='nearest')
            lat_range = slice(test.latitude.values[0], test.latitude.values[1])
            lon_range = slice(test.longitude.values[0], test.longitude.values[1])
        
        # slice before return
        ds = ds[var_names].sel(latitude=lat_range, longitude=lon_range).compute()
        datasets.append(ds)
    return xr.merge(datasets)