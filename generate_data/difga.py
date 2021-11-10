## @package Generate difga datasets
#
# - (1) calculate area or number of cells for each polygon
# - (2) calculate product of (1) and Q
# - (3) corrected Q: (2) minus sum of (2) of upper difga level
# - (4) dimensionless corrected Q [mm]: (3) divided by sum of (1) of upper difga level
#
# Create 3-dimensional arrays (data cubes), where dim0 is x (columns), dim1 is y (rows) and dim2 is t (time)
import sys
import os
import h5py
import pandas as pd
import numpy as np
sys.path.append(os.getcwd() + '/..')
from pg.pg import db_connector as pg
#from tests.t_difga import t_difga

class difga_generator:
    def __init__(self, args):
        """ configure output """
        ## init test class
        #self.test = t_difga()
        ## command line arguments
        self.args = args
        ## configuration of parameter properties
        self.area_shape = None
        self.conf = {
            'parameters' : {
                'P' : {'id':10, 'decimals':2, 'dtype':'int32', 'nodata':-99999, 'to_hdf' : True, 'aggregation' : 'yearly_sum'},
                'RG2' : {'id':13, 'decimals':2, 'dtype':'int32', 'nodata':-99999, 'to_hdf' : True, 'aggregation' : 'yearly_sum'},
                'RG1' : {'id':12, 'decimals':2, 'dtype':'int32', 'nodata':-99999, 'to_hdf' : True, 'aggregation' : 'yearly_sum'},
                'RD' :{'id':16, 'decimals':2, 'dtype':'int32', 'nodata':-99999, 'to_hdf' : True, 'aggregation' : 'yearly_sum'},
                'QG2' : {'id':15, 'decimals':2, 'dtype':'int32', 'nodata':-99999, 'to_hdf' : True, 'aggregation' : 'yearly_sum'},
                'QG1' : {'id':14, 'decimals':2, 'dtype':'int32', 'nodata':-99999, 'to_hdf' : True, 'aggregation' : 'yearly_sum'},
                'REST' : {'id':17, 'decimals':2, 'dtype':'int32', 'nodata':-99999, 'to_hdf' : True, 'aggregation' : 'yearly_sum'},
                'DEF' : {'id':18, 'decimals':2, 'dtype':'int32', 'nodata':-99999, 'to_hdf' : True, 'aggregation' : 'yearly_sum'}
            },
            'difga' : {
                'difga_1' : {'idarea' : 12, 'difga_level' : 1},
                'difga_2' : {'idarea' : 13, 'difga_level' : 2},
                'difga_3' : {'idarea' : 14, 'difga_level' : 3},
                'difga_4' : {'idarea' : 15, 'difga_level' : 4}
            },
            'dbconfig' : {
                "db_host" : "192.168.0.194",
                "db_name" : "gwn_sachsen",
                "db_user" : "visdat",
                "db_password" : "9Leravu6",
                "db_port" : "9991"
            }
        }

    ## @brief Prepare data for a defined parameter, returns the corrected parameter
    #
    # @param args
    # @param parameter_config
    # @param area_ds, [difga_areas, difga_areas_sum, difga_upper_areas_sum]
    # @returns param_ds, 3-dimensioal array, shape is (t,x,y) where t is the time dimension (e.g. year)
    def prepare_parameter_data(self, args, parameter_config, area_ds):
        print('--> prepare parameter data')
        print(parameter_config)
        param_container = {}
        # areas
        difga_areas = area_ds[0]
        # read parameter dataset and calculate statistic defined by self.conf['aggregation'] for each difga level
        #[print('upper area count at difga_level', str(l), area_ds[2][str(l)][r][c]) for l in area_ds[2]]
        df_param = self.read_parameter_file(args, parameter_config)
        # assign difga parameter values to grid for each difga level and year
        parameter_dict = self.assign_parameter2grid(df_param, difga_areas)
        # calculate upper area cell count dependent on year
        upper_area_cellcount = self.get_upper_area_cellcount(parameter_dict, difga_areas, args)
        # calculate product of parameter value and area size
        mult_dict = self.multiply_area_parameter(area_ds, parameter_dict)
        # calculate sum of product(area x parameter) for upper difga level of each idarea_data
        sum_upperlevel_mult = self.sum_mult_upperlevel(area_ds, mult_dict)
        # get difference of mult_dict and sum_upperlevel_mult
        diff = self.get_diff(mult_dict, sum_upperlevel_mult)
        # adjust parameter as quotient of diff and (area_sum - difga_upper_areas_sum)
        p_corrected = self.adjust_parameter(diff, area_ds, upper_area_cellcount, parameter_dict)
        # test result by recalculating result dataset to get orinial values
        self.test.recalculate_corrected_to_original(p_corrected, parameter_dict, difga_areas)
        # create data cube respecting dtype and decimals
        data_cube, first_year, len_years = self.create_data_cube(p_corrected, difga_areas, parameter_config)
        # save results as h5
        # save data to hd5
        # create path if not exists
        dst_fpath = self.args['dst_folder'] + '/parameters/' + str(parameter_config['id']) + '/'
        if not os.path.exists(dst_fpath):
            os.makedirs(dst_fpath)
        # set file name
        dtype = parameter_config['dtype']
        fname = str(parameter_config['id']) + '_' + str(self.args['scenario_id']) + '_' + str(self.args['level_id']) + '.' + str(dtype) + '.' + str(parameter_config['decimals']) + '.nd.h5'
        dst_fpath = dst_fpath + fname
        # container for some configurations to save as hdf5
        save_config = {}
        save_config['dataset'] = data_cube
        save_config['dst_path'] = dst_fpath
        save_config['ds_name'] = 'table'
        save_config['create_year_scale'] = True
        save_config['start_year'] = first_year
        save_config['n_years'] = len_years
        save_config['x'] = area_ds[2]
        save_config['y'] = area_ds[3]
        #print(save_config)
        self.save_as_h5(save_config, True, None)


    ## @brief Prepare parameter data regarding overlapping difga areas
    #
    # @param args, command line arguments
    # @returns ds_area, n-dimensional numpy arrays, [difga_areas, difga_areas_sum, difga_upper_areas_sum]
    # @returns conf, dict: parameter configuration
    def prepare_area_data(self, args):
        print('--> prepare parameter data regarding overlapping difga areas')
        print('reading difga areas...')
        # read difga area levels
        difga_areas, x, y = self.difga_areas_read(args)
        # calculate number of cells / area size for each idarea_data
        difga_areas_sum = self.difga_areas_calulate_cells(difga_areas, args)
        # calculate number of cells of upper difga level for each idarea_data
        #difga_upper_areas_sum = self.difga_areas_upperlevel_cellcount(difga_areas, args)

        return [difga_areas, difga_areas_sum, x, y], self.conf

    ## @brief Read difga areas from netcdf
    #
    # @param args, command line arguments
    # @returns ds_param, n-dimensional numpy array
    def difga_areas_read(self, args):
        print('--> get difga areas')
        fileContainer = []
        difga_areas = {}
        difga_zeros = np.array([])
        # loop through difga levels and get difga files
        for difga in self.conf['difga']:
            path = args['dst_folder'] + '/areas/'
            fn = str(self.conf['difga'][difga]['idarea']) + '_' + str(args['level_id'])

            # get file pathes
            if os.path.exists(path):
                for r, d, f in os.walk(path):
                    for file in f:
                        if fn in file and not '.aux.xml' in file:
                            print('OK, ', difga ,'file found ', file, 'in', path)
                            fname = file
                            fileContainer.append([r,fname, str(self.conf['difga'][difga]['difga_level'])])
            else:
                sys.exit('ERROR: area path not found' + str(path))

        # loop through fileContainer
        for p, fn, l in fileContainer:
            fpath = p + '/' + fn
            # read file
            f = h5py.File(fpath, 'r')
            area_ds = f['Band1'][:]
            x = f['x']
            y = f['y']
            self.area_shape = area_ds.shape
            f.close()
            # create a base area with zeros
            if len(difga_zeros) == 0:
                difga_zeros = np.zeros(self.area_shape)
                print(difga_zeros.shape)
                difga_areas[0] = difga_zeros
            difga_areas[l] = area_ds
        print('shape of difga_areas:', [['level ' + str(l), difga_areas[l].shape] for l in difga_areas])
        return difga_areas, x, y

    ## @brief Calculate the sum of number of cells for each idarea_data
    #
    # @param difga_areas
    # @param args
    def difga_areas_calulate_cells(self, difga_areas, args):
        # calculate number of cells for each idarea_data
        print('--> calculate number of cells for each idarea_data')
        difga_sum_area = {}
        for difga_level in difga_areas:
            a = difga_areas[difga_level]
            if str(difga_level) != str(0):
                _,ids,c = np.unique(a, return_counts=True, return_inverse=True)
                r = c[ids].reshape(a.shape)
                # replace nodata cells with 0
                difga_sum_area[difga_level] = np.where(a>=0, r, 0)
            else:
                difga_sum_area[str(difga_level)] = a

            #print('level', difga_level, np.unique(difga_areas[difga_level], return_counts=True))

        #sys.exit()
        return difga_sum_area

    ## @brief Calculate for each idarea_data the number of cells of upper difga level
    #
    # @param difga_areas
    # @param args
    def difga_areas_upperlevel_cellcount(self, difga_areas, args):
        # calculate for each idarea_data the number of cells of upper difga level
        print('--> calculate for each idarea_data the number of cells of upper difga level')
        difga_upper_areas_sum = {}
        levels = []
        [levels.append(int(i)) for i in difga_areas.keys()]
        for difga_level in difga_areas:
            if str(difga_level) != str(0):
                # a is the current difga level
                a = difga_areas[difga_level]
                #if str(difga_level) != str(4) :
                if (int(difga_level)+1) in levels:
                    # upper is the upper difga level
                    upper_ids = difga_areas[str(int(difga_level) + 1)]
                    # set cells of upper to number 1
                    upper = np.where(upper_ids>=0, 1, 0)
                    # uniques of current difga level
                    a_uniques = np.unique(a)
                    # get uniques of a where upper == 1
                    c = np.where(upper == 1, a, -99999)
                    # get number / count of upper cells
                    uniques = np.unique(c, return_counts=True)
                    i = 0
                    r = np.zeros(a.shape)
                    for u in uniques[0]:
                        if u >= 0:
                            r = np.where(a == u, uniques[1][i], r)
                        i = i + 1

                #if str(difga_level) == str(4):
                if (int(difga_level)+1) not in levels:
                    #upper = difga_areas[0]
                    r = np.zeros(difga_areas[str(0)].shape)

                difga_upper_areas_sum[difga_level] = r
        print('shape of difga_upper_areas_sum:', [['level ' + str(l), difga_upper_areas_sum[l].shape] for l in difga_upper_areas_sum])

        #sys.exit()
        return difga_upper_areas_sum

    ## @brief Calculate for each idarea_data and each year the number of cells of upper difga level
    #
    # take into account the change of presence of difga levels between years
    #
    # @param difga_areas
    # @param parameter_dict
    # @param args
    # @returns upper_area_cellcount dictionary
    def get_upper_area_cellcount(self, parameter_dict, difga_areas, args):
        print('calculate upper area cellcount dependent on year')
        upper_area_cellcount = {}

        for year in parameter_dict:
            # get difga difga
            print(year)
            levels = []
            [levels.append(int(i)) for i in parameter_dict[str(year)].keys()]
            areas = {}
            areas[str(0)] = difga_areas[0]
            for l in levels:
                areas[str(l)] = difga_areas[str(l)]

            cellcount = self.difga_areas_upperlevel_cellcount(areas, args)
            upper_area_cellcount[str(year)] = cellcount

        #print(upper_area_cellcount)

        return upper_area_cellcount

    ## @brief Read parameter files and return a dataframe respecting a time dimension for each difga_level
    #
    # aggregate data depending on args['aggregation']
    #
    # @param args
    # @param parameter, parameter configuration
    def read_parameter_file(self, args, parameter_config):
        print('--> read parameter file', parameter_config)
        scenario_id = args['scenario_id']
        param_id = parameter_config['id']
        path = args['src_folder'] + '/' + str(scenario_id) + '/' + str(param_id) + '/'
        # get idarea_data from database for each difga level
        difga_idarea_data = self.get_idarea_data_from_db(args)
        df_all = pd.DataFrame()
        # loop through parameter directory
        if os.path.exists(path):
            for r, d, f in os.walk(path):
                for file in f:
                    year = file.split('.')[0].split('_')[-1]
                    if year > args['max_year']:
                        break

                    df_param = pd.read_hdf(path + file, key='table').reset_index()
                    df_param['index'] = df_param['index']
                    # get a column for each month
                    df_param[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']] = df_param.values_block_0.str.split(expand=True,)
                    # remove column values_block_0
                    df_param = df_param.drop(columns=['values_block_0'])
                    # set nan values
                    df_param = df_param.set_index(['index']).astype('float').replace(-99999, np.nan)
                    # calculate yearly sum
                    if parameter_config['aggregation'] == 'yearly_sum':
                        df = df_param.sum(axis=1).reset_index().rename(columns={0:'value'})
                        df['year'] = year
                    # merge idarea, idarea_data and difga_level
                    df_merge = pd.merge(df, difga_idarea_data, how='inner', left_on='index', right_on='descr', sort=False)
                    df_all = pd.concat([df_all, df_merge])
        print('final parameterid ' + str(param_id) + ' dataframe')
        print('keys', df_all.shape, df_all.keys())
        print('min year', df_all['year'].min())
        print('max year', df_all['year'].max())
        #print(df_all.loc[df_all['difga_level'] == 1])
        #sys.exit()
        return df_all

    ## @brief Get all idarea_data from database for each difga level
    #
    # @param args
    # @returns dataframe
    def get_idarea_data_from_db(self, arg):
        print('--> get idarea_data for each difga level')
        # get difga idarea
        difga_ids = []
        [difga_ids.append(self.conf['difga'][i]['idarea']) for i in self.conf['difga']]
        # query database
        db = pg()
        dbconf = db.dbConfig(self.conf['dbconfig'])
        dbcon = db.dbConnect()
        sql = 'select idarea, idarea_data, area_data as gkz, description_text from spatial.area_data '
        sql += 'where idarea = any(array[' + str(difga_ids) + ']) order by idarea, idarea_data'
        res = db.tblSelect(sql)
        db.dbClose()
        # store result in dataframe
        df_difga_idarea_data = pd.DataFrame(res[0], columns={0,1,2,3}).rename(columns={0:'idarea', 1:'idarea_data', 2:'gkz', 3:'descr'})
        df_difga_idarea_data['difga_level'] = None
        # set difga level id
        for i in self.conf['difga']:
            df_difga_idarea_data.loc[df_difga_idarea_data['idarea'] == self.conf['difga'][i]['idarea'], 'difga_level'] = self.conf['difga'][i]['difga_level']

        print(df_difga_idarea_data.keys())

        return df_difga_idarea_data

    ## @brief Assign difga parameter value to raster for each difga level and year
    #
    # @param df_param
    # @param difga_areas
    # @returns parameter_dict
    def assign_parameter2grid(self, df_param, difga_areas):
        print('--> assign values to difga level grids and return a dict with 3d array for each year')
        # parameter container
        param_container = {}
        # get unique years
        years = pd.unique(df_param['year'])
        # loop over years
        for year in years:
            param_container[str(year)] = {}
            df_year = df_param[df_param['year'] == year]
            difga_levels = sorted(pd.unique(df_year['difga_level']), reverse=True)
            print(year, difga_levels)
            # set parameter value for a idarea and difga level
            for l in difga_levels:
                # get parameter values for difga level l
                df_year_l = df_year.loc[df_year['difga_level'] == l]
                # get difga area for level l
                difga_l = difga_areas[str(l)]
                param_l = np.array(difga_l, copy=True).astype(float)
                # get uniques
                u_idarea_data = np.unique(difga_l)
                u_idarea_data = u_idarea_data[u_idarea_data >= 0]
                u_idarea_data_year_l = pd.unique(df_year_l['idarea_data'])
                # replace idarea_id by parameter value or set a nodata value
                for id in u_idarea_data:
                    if id in u_idarea_data_year_l:
                        val = df_year_l['value'].loc[df_year_l['idarea_data'] == id]
                    else:
                        val = np.nan
                    param_l[param_l==id] = val
                # append dataset to parameter container)
                param_container[str(year)][str(l)] = param_l

        print(len(param_container), 'years')
        #sys.exit()
        return param_container

    ## @brief Multiply area and parameter of each difga level
    #
    # @param area_ds, [difga_areas, difga_areas_sum, difga_upper_areas_sum]
    # @param parameter_dict
    # @returns mult_dict
    def multiply_area_parameter(self, area_ds, parameter_dict):
        print('--> multiply area and parameter of difga levels')
        mult_dict = {}
        difga_areas_sum = area_ds[1]
        # loop over years
        for year in parameter_dict:
            mult_dict[str(year)] = {}
            for difga_level in parameter_dict[year]:
                ds_param = parameter_dict[year][difga_level]
                ds_area_sum = difga_areas_sum[difga_level]
                ds_mult = np.where(ds_param >= 0, np.multiply(ds_param, ds_area_sum), 0)
                mult_dict[str(year)][str(difga_level)] = ds_mult

        #sys.exit()
        return mult_dict

    ## @brief Calculate sum of product(area x parameter) for upper difga level of each idarea_data
    #
    # product(area x parameter) is the result of multiply_area_parameter()
    # @param area_ds, [difga_areas, difga_areas_sum, difga_upper_areas_sum]
    # @param mult_dict
    # @returns upper_sum_mult
    def sum_mult_upperlevel(self, area_ds, mult_dict):
        print('--> calculate sum of product(area x parameter) for upper difga level of each idarea_data ')
        difga_areas = area_ds[0]
        difga_upper_areas_sum = {}
        # loop over years
        for year in mult_dict:
            print(year)
            difga_upper_areas_sum[str(year)] = {}
            # loop over difga levels
            for difga_level in difga_areas:
                if str(difga_level) != str(0):
                    print('->difga_level', difga_level)
                    base_level = difga_areas[difga_level]
                    upper_difga_level = int(difga_level) + 1
                    difga_levels_parameter = np.fromiter(mult_dict[year].keys(), dtype=int)
                    if upper_difga_level <= 4 and upper_difga_level in difga_levels_parameter:
                        # get mult array (product of area and parameter) for year and level
                        p = mult_dict[year][str(upper_difga_level)]
                        # get upper difga areas
                        upper_areas = difga_areas[str(upper_difga_level)]
                        print('upper level of', difga_level, ' is', str(int(upper_difga_level)))
                        # set cells of upper to number 1
                        upper_param = np.where(upper_areas>=0, p, 0)
                        # stack area and parameter (p x a) together and get uniques combination
                        a = pd.DataFrame(np.array([base_level.flatten(), upper_param.flatten()]).T)
                        a = a.rename(columns={0:'area',1:'value'})
                        uniques = a.groupby(['area','value']).size().reset_index()
                        uniques = uniques[uniques['area']>=0]
                        # sum of value groupby areas
                        sum = uniques.groupby(['area'])['value'].sum().reset_index()
                        # replace values where is no area
                        sum.loc[sum.area < 0, 'value'] = 0
                        # assign sum to raster
                        r = pd.merge(pd.DataFrame(a['area']), sum, how='left', left_on='area', right_on='area', sort=False)
                        # reshape r
                        r = np.array(r['value']).reshape(base_level.shape)
                        # set value to zero where upper area is < 0
                        #r = np.where(upper_areas <0, 0, r)

                    # handle level without upper level
                    if upper_difga_level > max(difga_levels_parameter) and int(difga_level) in difga_levels_parameter:
                        upper = difga_areas[0]
                        print('upper level of', difga_level, ' is', str(0))
                        r = np.zeros(base_level.shape)

                    difga_upper_areas_sum[str(year)][str(difga_level)] = r

        #sys.exit()
        return difga_upper_areas_sum

    ## @brief Get (parameter) difference of mult_dict and sum_upperlevel_mult
    #
    # @param mult_dict
    # @param sum_upperlevel_mult
    # @return diff, dict: corrected Q
    def get_diff(self, mult_dict, sum_upperlevel_mult):
        print('--> get difference')
        diff = {}
        for year in mult_dict:
            print(year)
            diff[str(year)] = {}
            for difga_level in mult_dict[str(year)]:
                diff[str(year)][str(difga_level)] = np.subtract(mult_dict[str(year)][str(difga_level)], sum_upperlevel_mult[str(year)][str(difga_level)])

        #sys.exit()
        return diff

    ## @brief Adjust parameter value of difga levels
    #
    # Divide corrected parameter by remaining area of current difga level
    def adjust_parameter(self, ds_diff, ds_areas, upper_area_cellcount, original_ds):
        print('--> adjust parameters of difga levels')
        ds_areas_sum = ds_areas[1]
        p_adjusted = {}
        p_final = {}
        # division value by area differences
        for year in ds_diff:
            print(year)
            ds_upper_areas_sum = upper_area_cellcount[str(year)]
            p_adjusted[str(year)] = {}
            levels = []
            [levels.append(int(i)) for i in ds_diff[str(year)].keys()]

            for difga_level in ds_diff[str(year)]:
                area_sum = ds_areas_sum[str(difga_level)]
                upper_area_sum = ds_upper_areas_sum[str(difga_level)]
                area_diff = np.subtract(area_sum, upper_area_sum)
                area_diff = np.where(area_diff > 0, area_diff, 0)

                p_diff = ds_diff[str(year)][str(difga_level)]
                p_diff = np.where(np.isnan(p_diff), 0, p_diff)
                p_diff = np.where(p_diff < 0, 0, p_diff)

                r = np.divide(p_diff, area_diff)

                p_adjusted[str(year)][str(difga_level)] = np.where(np.isnan(r), np.nan, r)

        # adjust zero and nan values
        for year in p_adjusted:
            a_final = np.empty(p_adjusted[str(year)][str(difga_level)].shape)
            levels = []
            [levels.append(int(i)) for i in p_adjusted[str(year)].keys()]
            levels = sorted(levels, reverse=False)
            for difga_level in levels:
                a_final = np.where((p_adjusted[str(year)][str(difga_level)] >=0) & (ds_areas[0][str(difga_level)] >= 0), p_adjusted[str(year)][str(difga_level)], a_final)
                #918 1563 level 1 nr 15
                #997 1732 level 2 nr 2 (level 1: 15)
                #315 772 level 1 nr 1
                #c=315
                #r=772
                #print('adju', year, difga_level, p_adjusted[str(year)][str(difga_level)][c][r])
                #print('orig', year, difga_level, original_ds[str(year)][str(difga_level)][c][r])
            #print('final', year, a_final[c][r])
            a_final = np.where(a_final == 0, np.nan, a_final)
            #print('final', year, a_final[c][r])
            p_final[str(year)] = a_final

        #sys.exit()
        return p_final

    ## @brief Save an n-dimensional array as hdf5 file
    #
    # @param data dict, dictionary wich contains the relevant data: fpath, dataset, name of dataset, dimensions, scale etc.
    # @param overwrite boolean, overwrite / replace file
    # @param groupName string, name of a group object (optional)
    def save_as_h5(self, data, overwrite, groupName):
        print('--> save as h5')
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

    ## @brief Create data cube as data structure to save
    #
    # @param dataset, p_corrected
    # @returns data_cube
    def create_data_cube(self, dataset, ds_area, param_config):
        print('--> Create data cube, 3d array ')
        n_years = len(dataset)
        first_year = None
        current_year_idx = 0
        decimals = param_config['decimals']
        dtype = param_config['dtype']
        nodata = param_config['nodata']

        print('number of years', n_years)
        shape_area = ds_area[0].shape
        shape = (shape_area[0], shape_area[1], n_years)
        data_cube = np.zeros((shape), dtype)
        print('shape of ndim array', data_cube.shape, data_cube.dtype)
        # loop through years
        for year in dataset:
            print(year)
            if first_year == None:
                first_year = year
            ds_year = dataset[str(year)]
            # format decimals
            ds_year = np.round(ds_year * (10**decimals),0)
            # assign an specified value as nan
            ds_year[np.isnan(ds_year)] = nodata
            ds_year = ds_year.astype(dtype)
            # set values into cube
            data_cube[:,:, current_year_idx] = ds_year
            # increase current year index
            current_year_idx = current_year_idx + 1

        return data_cube, first_year, n_years
