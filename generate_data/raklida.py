## @package Generate raklida raster datasets
#
# Create 3-dimensional arrays (data cubes), where dim0 is x (columns), dim1 is y (rows) and dim2 is t (time)
import sys
import os
import h5py
import pandas as pd
import numpy as np
sys.path.append(os.getcwd() + '/..')
from pg.pg import db_connector as pg
import rasterio
#from tests.t_difga import t_difga
import subprocess

class raklida_generator:
    def __init__(self, args):
        """ configure output """
        ## command line arguments
        self.args = args
        ## configuration of parameter properties
        self.conf = {
            'raklida_messungen' : {
                'ETP' :{'id':8, 'decimals':2, 'dtype':'int32', 'to_hdf' : True, 'aggregation' : 'yearly_sum'},
                'GRV' : {'id':30, 'decimals':2, 'dtype':'int32', 'to_hdf' : True, 'aggregation' : 'yearly_sum'}
            },
            'raklida_referenz' : {
                'SN' : {'id':6, 'decimals':2, 'dtype':'int32', 'to_hdf' : True, 'aggregation' : 'yearly_sum'}, # summer precipitation
                'WN' : {'id':7, 'decimals':2, 'dtype':'int32', 'to_hdf' : True, 'aggregation' : 'yearly_sum'}, # winter precipitation
                'RRK' :{'id':10, 'decimals':2, 'dtype':'int32', 'to_hdf' : True, 'aggregation' : 'yearly_sum'},
                'TM0' : {'id':9, 'decimals':2, 'dtype':'int32', 'to_hdf' : True, 'aggregation' : 'yearly_mean'}
            },
            'raklida_wettreg66' : {
                'SN' : {'id':6, 'decimals':2, 'dtype':'int32',  'to_hdf' : True, 'aggregation' : 'yearly_sum'},
                'WN' : {'id':7, 'decimals':2, 'dtype':'int32', 'to_hdf' : True, 'aggregation' : 'yearly_sum'},
                'ETP' :{'id':8, 'decimals':2, 'dtype':'int32', 'to_hdf' : True, 'aggregation' : 'yearly_sum'},
                'RRK' : {'id':10, 'decimals':2, 'dtype':'int32', 'to_hdf' : True, 'aggregation' : 'yearly_sum'},
                'TM0' : {'id':9, 'decimals':2, 'dtype':'int32', 'to_hdf' : True, 'aggregation' : 'yearly_mean'}
            },
            'dbconfig' : {
                "db_host" : "192.168.0.194",
                "db_name" : "gwn_sachsen",
                "db_user" : "visdat",
                "db_password" : "9Leravu6",
                "db_port" : "9991"
            }
        }

    ## @brief Create a dataset for each raklida_messungen parameter
    #
    # @param area_ds, pointer to area dataset read by get_area
    def create_raklida_messungen(self, args, years_range, params):
        print('--> Create raklida_messungen parameter datasets')
        parameter_cube = {}
        # loop over year and parameter
        for p in params:
            parameter_cube[p] = {}
            files_per_year = {}

            # (0) get files for each year
            for y in years_range:
                files_per_year[y] = []
                
                for f in args['asc_files']:
                    fn_part = f.split('/')[-1].split('_')[-1]
                    if p in f:
                        if str(y) in fn_part[0:4]:
                            files_per_year[y].append(f)
                       
                            
                if len(files_per_year[y]) != 12:
                        
                    print ('error', files_per_year[y])
                    sys.exit()
                
                    
            # (1) summarize or average monthes for each year
            print(p)
            asc_files, netcdf_files = [], []
            for y in files_per_year:
                print(y)
                # get fpath for yearly ascii file for mean or sum
                asc_files.append(self.get_yearly_param(args, files_per_year[y], p, y))
                #break
            # (2) resampling to 100x100m and reprojecting using gdalwarp, output format is netcdf
            for fname in asc_files:
                netcdf_files.append(self.resample_grid(fname))
                #break

            #break
            # (3) build 3d cube
            self.build_3D_cube(p, args)
            #break

        return parameter_cube

    ## @brief Create a dataset for each raklida_referenz parameter
    #
    # @param area_ds, pointer to area dataset read by get_area
    def create_raklida_referenz(self, args, years_range, params):
        print('--> Create raklida_messungen parameter datasets')
        parameter_cube = {}
        # loop over year and parameter
        for p in params:
            parameter_cube[p] = {}
            files_per_year = {}

            # (0) get files for each year
            for y in years_range:
                files_per_year[y] = []
                
                for f in args['asc_files']:
                    fn_part = f.split('/')[-1].split('_')[-1]
                    if p in f:
                        if str(y) in fn_part[0:4]:
                            files_per_year[y].append(f)
                       
                            
                if len(files_per_year[y]) != 12:
                        
                    print ('error', files_per_year[y])
                    sys.exit()
            
            # (1) summarize or average monthes for each year
            print(p)
            asc_files, netcdf_files = [], []
            for y in files_per_year:
                print(y)
                # get fpath for yearly ascii file for mean or sum
                asc_files.append(self.get_yearly_param(args, files_per_year[y], p, y))
                #break
            # (2) resampling to 100x100m and reprojecting using gdalwarp, output format is netcdf
            for fname in asc_files:
                netcdf_files.append(self.resample_grid(fname))
                #break

            #break
            # (3) build 3d cube
            self.build_3D_cube(p, args)
            #break

        return parameter_cube

    ## @brief Create a dataset for each raklida_referenz summer and winter precipitation
    #
    # @param area_ds, pointer to area dataset read by get_area
    def create_raklida_referenz_sn_wn(self, args, parameter):
        print('--> Create raklida_referenz parameter datasets for summer and winter precipitation ', parameter)
        # get RRK datasets
        rrk_files = []
        for f in args['asc_files']:
            if 'RRK' in f:
                rrk_files.append(f)
        args['asc_files'] = rrk_files
        years_range = self.get_years_range(args)
        parameter_cube = {}
        # loop over year and parameter
        p = 'RRK'
        parameter_cube[p] = {}
        files_per_year = {}

        # (0) get files for summer or winter months
        for y in years_range:
            files_per_year[y] = []
            for f in args['asc_files']:
                fn_part = f.split('/')[-1].split('_')[-1]
                m_part = fn_part.split('.')[0][-4:]
                if str(y) in fn_part[0:4]:
                    if parameter == 'SN':
                        months = ['0400','0500','0600','0700','0800','0900']
                    if parameter == 'WN':
                        months = ['0100','0200','0300','1000','1100','1200']
                    for m in months:
                        if m in m_part:
                            files_per_year[y].append(f)
                            
            if len(files_per_year[y]) != 6:
                print ('error', files_per_year[y])
                sys.exit()
                    
        # (1) summarize or average monthes for summer or winter
        asc_files, netcdf_files = [], []
        for y in files_per_year:
            print(y)
            # get fpath for yearly ascii file for mean or sum
            asc_files.append(self.get_yearly_param(args, files_per_year[y], p, y))
            #break
        # (2) resampling to 100x100m and reprojecting using gdalwarp, output format is netcdf
        for fname in asc_files:
            netcdf_files.append(self.resample_grid(fname))
        # (3) build 3d cube
        self.build_3D_cube_ns_ws(p, args, parameter)

    ## @brief Create a dataset for each raklida_wettreg66 parameter
    #
    # @param area_ds, pointer to area dataset read by get_area
    def create_raklida_wettreg66(self, args, years_range, params):
        print('--> Create raklida_wettreg66 parameter datasets')
        parameter_cube = {}
        # loop over year and parameter
        for p in params:
            parameter_cube[p] = {}
            files_per_year = {}

            # (0) get files for each year
            for y in years_range:
                files_per_year[y] = []
                for f in args['asc_files']:
                    fn_part = f.split('/')[-1].split('_')[-1]
                    if p in f:
                        if str(y) in fn_part[0:4]:
                            files_per_year[y].append(f)
                       
                            
                if len(files_per_year[y]) != 12:
                        
                    print ('error', files_per_year[y])
                    sys.exit()
                    
            # (1) summarize or average monthes for each year
            print(p)
            asc_files, netcdf_files = [], []
            for y in files_per_year:
                print(y)
                # get fpath for yearly ascii file for mean or sum
                asc_files.append(self.get_yearly_param(args, files_per_year[y], p, y))
                #break
            # (2) resampling to 100x100m and reprojecting using gdalwarp, output format is netcdf
            for fname in asc_files:
                netcdf_files.append(self.resample_grid(fname))
                #break

            #break
            # (3) build 3d cube
            self.build_3D_cube(p, args)
            #break

        return parameter_cube

    ## @brief Create a dataset for each raklida_referenz summer and winter precipitation
    #
    # @param area_ds, pointer to area dataset read by get_area
    def create_raklida_wettreg66_sn_wn(self, args, parameter):
        print('--> Create raklida_wettreg66 parameter datasets for summer and winter precipitation ', parameter)
        # get RRK datasets
        rrk_files = []
        for f in args['asc_files']:
            if 'RRK' in f:
                rrk_files.append(f)
        args['asc_files'] = rrk_files
        years_range = self.get_years_range(args)
        parameter_cube = {}
        # loop over year and parameter
        p = 'RRK'
        parameter_cube[p] = {}
        files_per_year = {}

        # (0) get files for summer or winter months
        for y in years_range:
            files_per_year[y] = []
            for f in args['asc_files']:
                fn_part = f.split('/')[-1].split('_')[-1]
                m_part = fn_part.split('.')[0][-4:]
                if str(y) in fn_part[0:4]:
                    if parameter == 'SN':
                        months = ['0400','0500','0600','0700','0800','0900']
                    if parameter == 'WN':
                        months = ['0100','0200','0300','1000','1100','1200']
                    for m in months:
                        if m in m_part:
                            files_per_year[y].append(f)
                            
            if len(files_per_year[y]) != 6:
                print ('error', files_per_year[y])
                sys.exit()
                
        # (1) summarize or average monthes for summer or winter
        asc_files, netcdf_files = [], []
        for y in files_per_year:
            print(y)
            #print(files_per_year[y])
            # get fpath for yearly ascii file for mean or sum
            asc_files.append(self.get_yearly_param(args, files_per_year[y], p, y))
            #break
        # (2) resampling to 100x100m and reprojecting using gdalwarp, output format is netcdf
        for fname in asc_files:
            netcdf_files.append(self.resample_grid(fname))
        # (3) build 3d cube
        self.build_3D_cube_ns_ws(p, args, parameter)

    ## @brief Extract years range from filenames
    def get_years_range(self, args):
        src_folder = args['src_folder']
        print('--> Extract years span from filenames in ', src_folder)
        years_range = []
        for f in args['asc_files']:
            year, month = self.get_year_month_from_fname(f)
            if year not in years_range:
                years_range.append(year)

        print(years_range)
        return years_range

    ## @brief Get unique parameter names from filenames in a folder
    def get_unique_parameters_from_folder(self, args):
        src_folder = args['src_folder']
        print('--> Get uniques parameter names from filenames in ', src_folder)
        # get unique parameters
        u_pnames = []
        for f in args['asc_files']:
            pname, idparam = self.get_parameter_from_fname(args, f)
            #print(pname, idparam)
            if not pname in u_pnames and pname != None:
                u_pnames.append(pname)
        print('unique parameters ' + str(u_pnames))
        return u_pnames

    ## @brief Get parameter name and id from filename
    #
    # @param filename: string
    # @returns pname, id: integer
    def get_parameter_from_fname(self, args, file):
        #print('--> get parameter name and id from filename')
        pname, idparam = None, None
        for p in self.conf[args['data_type']]:
            #print(p, 'to_hdf', self.conf[args['data_type']][p]['to_hdf'])
            if p in file.split('/')[-1] and self.conf[args['data_type']][p]['to_hdf'] == True:
                idparam = self.conf[args['data_type']][p]['id']
                pname = p

        #if pname == None or idparam == None:
        #    sys.exit('ERROR: Missing parameter name or parameter id or to_hdf is set to False')

        return pname, idparam

    ## @brief Read asc files
    #
    # @param src, string : source folder
    # @return dbf_list, list: list of files
    def read_asc(self, src):
        print('--> read raklida asc files')
        files = []

        # get raklida asc files
        if 'Raklida' in src:
            for r, d, f in os.walk(src):
                print(r)
                if not 'py_tmp' in r: # exclude py_tmp directory
                    for file in f:
                        if '.asc' in file:
                            if not 'lock' in file: # exlude locked files
                                if not '.txt' in file:
                                    if not '.aux' in file:
                                        files.append(os.path.join(r, file))

        if len(files) > 0:
            print('OK, found %d asc files' % len(files))
            #for i in files:
            #    print(i)
        else:
            sys.exit('ERROR: No asc files found')
        #sys.exit()
        return files

    ## @brief Get year and month from filename
    #
    # @param filename: string
    # @returns year, month: integer
    def get_year_month_from_fname(self, file):
        #print('--> get year and month from filename')
        year, month = None, None
        months = {1:'jan', 2:'feb', 3:'mar', 4:'apr', 5:'may', 6:'jun', 7:'jul', 8:'aug', 9:'sep', 10:'oct', 11:'nov', 12:'dec'}

        yearmonth = file.split('.')[0].split('_')[-1]
        year = int(yearmonth[0:4])

        if int(yearmonth[4:6]) in range(1,13):
            month = int(yearmonth[4:6])

        if year == None or month == None:
            sys.exit('ERROR: Missing year or month')

        return year, months[month]

    ## @brief Calculate sum or average for one year
    def get_yearly_param(self, args, file_list, parameter, year):
        p_year = None
        # get_geoinformation_from_asc
        geoinformation = self.get_geoinformation_from_asc(file_list[0])
        # get shape
        spatial_shape = rasterio.open(file_list[0]).read(1).shape
        time_shape = len(file_list)
        shape = (spatial_shape[0], spatial_shape[1], time_shape)
        # create ndarray
        p_array = np.zeros((shape))
        # read asc files
        i = 0
        for f in file_list:
            # get nodata value
            raster = rasterio.open(f).read(1) # read file by rasterio
            nodata = geoinformation['nodata']
            raster[raster == nodata] = np.nan
            p_array[:,:,i] = raster
            i = i + 1
        print('shape', p_array.shape, self.conf[args['data_type']][parameter]['aggregation'])
        # get aggregation method
        if self.conf[args['data_type']][parameter]['aggregation'] == 'yearly_sum':
            p_year = np.nansum(p_array, axis = 2)
            #print(p_array[100,100,:])
            #print(p_year[100,100])
            #sys.exit()
        if self.conf[args['data_type']][parameter]['aggregation'] == 'yearly_mean':
            p_year = np.nanmean(p_array, axis = 2)
        # set nan values
        p_year = np.where(~np.isnan(p_array[:,:,0]), p_year, geoinformation['nodata'])
        print('result shape', p_year.shape, p_year[100,100])
        # write to asc grid file
        dstpath = args['src_folder'] + '/py_tmp/'
        if not os.path.exists(dstpath):
            os.makedirs(dstpath)
        dst = dstpath + args['data_type'] + '_' + parameter + '_' + str(year)+'.asc'
        self.write_asc_grid(geoinformation, p_year, dst)
        return dst

    ## @brief Get geoinformation from ascii grid, e.g. nodata value
    #
    # @param file, string
    # @returns dict
    def get_geoinformation_from_asc(self, file):
        #print('--> get nodata value from file')
        geoinformation = {}
        geoinformation['nodata'] = None
        geoinformation['ncols'] = None

        file = open(file, 'r')
        line_counter = 1
        for line in file.readlines():
            if line_counter == 7:
                break
            if 'nodata_value' in line:
                geoinformation['nodata'] = float((line.split(' '))[-1])
            elif 'ncols' in line:
                geoinformation['ncols'] = int((line.split(' '))[-1])
            elif 'nrows' in line:
                geoinformation['nrows'] = int((line.split(' '))[-1])
            elif 'xllcorner' in line:
                geoinformation['xllcorner'] = float((line.split(' '))[-1])
            elif 'yllcorner' in line:
                geoinformation['yllcorner'] = float((line.split(' '))[-1])
            elif 'cellsize' in line:
                geoinformation['cellsize'] = float((line.split(' '))[-1])
            line_counter = line_counter + 1
        file.close()
        if geoinformation['nodata'] == None:
            sys.exit('ERROR: Missing nodata value in asc file', file)

        return geoinformation

    ## @brief Create and save an ascii grid
    #
    def write_asc_grid(self, geoinformation, numpy_array, dst_folder):
        out = open(dst_folder, 'w+')
        out.write('ncols         %i\n' % geoinformation['ncols'])
        out.write('nrows         %i\n' % geoinformation['nrows'])
        out.write('xllcorner     %i\n' % geoinformation['xllcorner'])
        out.write('yllcorner     %i\n' % geoinformation['yllcorner'])
        out.write('cellsize      %i\n' % geoinformation['cellsize'])
        out.write('NODATA_value  %i\n' % geoinformation['nodata'])
        np.savetxt(out, numpy_array)
        out.close()

    ## @brief Generate netcdf file using gdalwarp for resampling and reprojecting
    #
    def resample_grid(self, fname):
        """ gdalwarp for resampling and reprojecting """
        s_srs = '-s_srs EPSG:31468 '
        t_srs = '-t_srs EPSG:25833 '
        of = '-of netCDF '
        co = '-co "COMPRESS=DEFLATE" -co "ZLEVEL=9" -co "FORMAT=NC4" '
        resamplingMethod = '-r bilinear '
        tr = '-tr 100 100 '
        te = '-te 278000.0 5561000.0 503000.0 5729000.0 '
        srcnodata = '-srcnodata -9999.0 '
        dstnodata = '-dstnodata -9999.0 '
        fname = fname.replace(' ', '\ ') # mask spaces
        src_path = fname + ' '
        dst_path = fname + '.100.25833.nc'

        gdal_string = 'gdalwarp -overwrite ' + s_srs + t_srs + of + co + resamplingMethod + tr + te + srcnodata + dstnodata + src_path + dst_path
        #print(gdal_string)

        p = subprocess.Popen('/bin/bash', shell=True, stdin=subprocess.PIPE)
        p.communicate(gdal_string.encode('utf8'))[0]
        del p

        return dst_path

    ## @brief Create a 3D array for a parameter
    def build_3D_cube(self, pname, args):
        """ """
        # read netcdf files_per_year for one parameter in a container
        nc_files = sorted(self.read_netcdf(args['src_folder'] + '/py_tmp/', pname))
        x, y = None, None
        # create cube
        # create a ndim array with shape (x,y,t)
        n_years = len(nc_files)
        current_year_idx = 0
        print('number of years', n_years)
        shape_spatial = h5py.File(nc_files[0], 'r')['Band1'].shape
        shape = (shape_spatial[0], shape_spatial[1], n_years)
        dtype = self.conf[args['data_type']][pname]['dtype']
        decimals = self.conf[args['data_type']][pname]['decimals']
        param_id = self.conf[args['data_type']][pname]['id']
        data_cube = np.zeros((shape), dtype)
        print('shape of ndim array', data_cube.shape, data_cube.dtype)
        years = self.get_years_range( args)
        start_year = years[0]
        # fill cube
        print('creating the cube...')
        for f in nc_files:
            #print('Reading netcdf file', f)
            r = h5py.File(f, 'r')['Band1'][:]
            # get x and y tables
            if x == None:
                x = h5py.File(f, 'r')['x']
                y = h5py.File(f, 'r')['y']
            # set nan
            r = np.where(r == -9999.0, np.nan, r)
            nodata_mask = np.isnan(r)
            # format decimals
            r = (r * (10**decimals)).round(0).astype(dtype)
            # set dtype
            r = r.astype(dtype)# assign an specified value as nan
            r[nodata_mask] = -99999 # append parameter matrix of current year  to data cube / ndarray
            #print(r)
            #sys.exit()
            data_cube[:,:, current_year_idx] = r
            # increase current year index
            current_year_idx = current_year_idx + 1

        # save data to hd5
        # create path if not exists
        dst_fpath = self.args['dst_folder'] + '/parameters/' + str(self.args['scenario_id']) + '/' + str(param_id) + '/'
        if not os.path.exists(dst_fpath):
            os.makedirs(dst_fpath)
        # set file name

        fname = str(param_id) + '_' + str(self.args['scenario_id']) + '_' + str(self.args['level_id']) + '.' + str(dtype) + '.' + str(decimals) + '.nd.h5'
        dst_fpath = dst_fpath + fname
        # container for some configurations to save as hdf5
        save_config = {}
        save_config['dataset'] = data_cube
        save_config['dst_path'] = dst_fpath
        save_config['ds_name'] = 'table'
        save_config['create_year_scale'] = True
        save_config['start_year'] = start_year
        save_config['n_years'] = n_years
        save_config['x'] = x
        save_config['y'] = y
        self.save_as_h5(save_config, True, None)

    ## @brief Create a 3D array for a parameter
    def build_3D_cube_ns_ws(self, pname, args, parameter):
        """ """
        print(parameter)
        # read netcdf files_per_year for one parameter in a container
        nc_files = sorted(self.read_netcdf(args['src_folder'] + '/py_tmp/', pname))
        x, y = None, None
        # create cube
        # create a ndim array with shape (x,y,t)
        n_years = len(nc_files)
        current_year_idx = 0
        print('number of years', n_years)
        shape_spatial = h5py.File(nc_files[0], 'r')['Band1'].shape
        shape = (shape_spatial[0], shape_spatial[1], n_years)
        dtype = self.conf[args['data_type']][parameter]['dtype']
        decimals = self.conf[args['data_type']][parameter]['decimals']
        param_id = self.conf[args['data_type']][parameter]['id']
        data_cube = np.zeros((shape), dtype)
        print('shape of ndim array', data_cube.shape, data_cube.dtype)
        years = self.get_years_range( args)
        start_year = years[0]
        # fill cube
        print('creating the cube...')
        for f in nc_files:
            #print('Reading netcdf file', f)
            r = h5py.File(f, 'r')['Band1'][:]
            # get x and y tables
            if x == None:
                x = h5py.File(f, 'r')['x']
                y = h5py.File(f, 'r')['y']
            # set nan
            r = np.where(r == -9999.0, np.nan, r)
            nodata_mask = np.isnan(r)
            # format decimals
            r = (r * (10**decimals)).round(0).astype(dtype)
            # set dtype
            r = r.astype(dtype)# assign an specified value as nan
            r[nodata_mask] = -99999 # append parameter matrix of current year  to data cube / ndarray
            #print(r)
            #sys.exit()
            data_cube[:,:, current_year_idx] = r
            # increase current year index
            current_year_idx = current_year_idx + 1

        # save data to hd5
        # create path if not exists
        dst_fpath = self.args['dst_folder'] + '/parameters/' + str(self.args['scenario_id']) + '/' + str(param_id) + '/'
        if not os.path.exists(dst_fpath):
            os.makedirs(dst_fpath)
        # set file name
        fname = str(param_id) + '_' + str(self.args['scenario_id']) + '_' + str(self.args['level_id']) + '.' + str(dtype) + '.' + str(decimals) + '.nd.h5'
        dst_fpath = dst_fpath + fname
        # container for some configurations to save as hdf5
        save_config = {}
        save_config['dataset'] = data_cube
        save_config['dst_path'] = dst_fpath
        save_config['ds_name'] = 'table'
        save_config['create_year_scale'] = True
        save_config['start_year'] = start_year
        save_config['n_years'] = n_years
        save_config['x'] = x
        save_config['y'] = y
        #print(save_config)
        #sys.exit()
        self.save_as_h5(save_config, True, None)

    ## @brief Read asc files
    #
    # @param src, string : source folder
    # @return dbf_list, list: list of files
    def read_netcdf(self, src, pname):
        print('--> read raklida netcdf files')
        files = []

        # get raklida asc files
        if 'Raklida' in src:
            for r, d, f in os.walk(src):
                if 'py_tmp' in r: # exclude py_tmp directory
                    for file in f:
                        if '.nc' in file and pname in file:
                            if not 'lock' in file: # exlude locked files
                                if not '.txt' in file:
                                    if not '.aux' in file:
                                        files.append(os.path.join(r, file))

        if len(files) > 0:
            print('OK, found %d netcdf files' % len(files), pname)
            #for i in files:
            #    print(i)
        else:
            sys.exit('ERROR: No netcdf files found')

        return files

    ## @brief Save an n-dimensional array as hdf5 file
    #
    # @param data dict, dictionary wich contains the relevant data: fpath, dataset, name of dataset, dimensions, scale etc.
    # @param overwrite boolean, overwrite / replace file
    # @param groupName string, name of a group object (optional)
    def save_as_h5(self, data, overwrite, groupName):
        print('--> save as h5')
        print('--> ' + data['dst_path'])
        path = data['dst_path']
        dataset = data['dataset']
        ds_name = data['ds_name']
        x = data['x']
        y = data['y']

        try:
            if overwrite == True:
                f = h5py.File(path, 'w')
            if overwrite == False:
                f = h5py.File(path, 'a')

            # create a group object
            if groupName != None:
                grp = f.create_group(groupName)
                ds = grp.create_dataset(ds_name, (dataset.shape) , dtype=dataset.dtype, compression="gzip", compression_opts=9)
                #ds = grp.create_dataset(ds_name, (dataset.shape) , dtype=dataset.dtype)
                ds[:] = dataset
            else:
                #ds = f.create_dataset(ds_name, (dataset.shape) , dtype=dataset.dtype, compression="gzip", compression_opts=9)
                ds = f.create_dataset(ds_name, (dataset.shape) , dtype=dataset.dtype)
                ds[:] = dataset

            if data['create_year_scale'] == True:
                years_dim = np.arange(int(data['start_year']),int( data['start_year'])+int(data['n_years']))
                f.create_dataset('year', data = years_dim)
            f.create_dataset('x', data = x)
            f.create_dataset('y', data = y)
            f.close()
        except:
            sys.exit("ERROR: " + str(sys.exc_info()))
