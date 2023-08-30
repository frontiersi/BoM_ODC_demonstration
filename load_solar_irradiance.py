import xarray as xr
import numpy as np
import pandas as pd
from tqdm import tqdm
# function to load solar irradiance data

def load_solar_irradiance(lat, lon, time, var_names = ['daily_integral_of_surface_global_irradiance'],
                          product_baseurl = 'https://dapds00.nci.org.au/thredds/dodsC/rv74/satellite-products/arc/der/himawari-ahi/solar/p1d/latest',pixel_buffer=0.02):
    '''
    Retrive solar radiation products from THREDDS server
    Parameters:
        lat: tuple, latitude range
        lon: tuple, longitude range
        var_names: list of strings, mesurements/observations
        product_baseurl: string, base url of the THREDDS directory
        pixel_buffer: float, a buffer of distance in degrees applied to slicing the dataset so that it includes the boundary pixels
    '''
    daterange = pd.date_range(time[0],time[1])
    datasets = []
    print('loading daily observations...')
    for single_date in tqdm(daterange):
        # retrive year, month and day for directory construction
        year=str(single_date.year)
        month=str(single_date.month).zfill(2)
        day=str(single_date.day).zfill(2)
        product_url='/'.join([product_baseurl,year,month,day])+'/IDE02326.'+year+month+day+'0000.nc'
        # print(product_url)
        # data is loaded lazily through OPeNDAP
        with xr.open_dataset(product_url) as ds:      
            # slice before return
            ds_sliced = ds[var_names].sel(latitude=slice(lat[0],lat[1]+pixel_buffer), longitude=slice(lon[0],lon[1]+pixel_buffer)).compute()
            datasets.append(ds_sliced)
    print('merging datasets...')
    return xr.merge(datasets)