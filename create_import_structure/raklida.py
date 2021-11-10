## @package Read raklida datasets
import sys
import os
import h5py
import pandas as pd
import numpy as np
import rasterio

class raklida_import:
    def __init__(self, args):
        """ configure output """
        ## command line arguments
        self.args = args
        ## configuration of parameter properties
        self.conf = {
            'raklida_messungen' : {
                'parameters' : {
                   'ETP' : {'id':8, 'decimals':2, 'dtype':'int32', 'to_hdf' : True},
                   'GRV' : {'id':30, 'decimals':2, 'dtype':'int32', 'to_hdf' : True}
                }
            },
            'raklida_referenz' : {
                 'parameters' : {
                   'RRK' :{'id':10, 'decimals':2, 'dtype':'int32', 'to_hdf' : True},
                   'TM0' : {'id':9, 'decimals':2, 'dtype':'int32', 'to_hdf' : True}
                }
            },
            'raklida_wettreg66' : {
                 'parameters' : {
                   'ETP' :{'id':8, 'decimals':2, 'dtype':'int32', 'to_hdf' : True},
                   'RRK' : {'id':10, 'decimals':2, 'dtype':'int32', 'to_hdf' : True},
                   'TM0' : {'id':9, 'decimals':2, 'dtype':'int32', 'to_hdf' : True}
                }
            },
            'raklida_referenz_derived' : {
                'parameters' : {
                    'pi_summer' : {'id':6, 'decimals':2, 'dtype':'int32', 'to_hdf' : True},
                    'pi_winter' : {'id':7, 'decimals':2, 'dtype':'int32', 'to_hdf' : True}
                }
            },
            'raklida_wettreg66_derived' : {
                'parameters' : {
                    'pi_summer' : {'id':6, 'decimals':2, 'dtype':'int32', 'to_hdf' : True},
                    'pi_winter' : {'id':7, 'decimals':2, 'dtype':'int32', 'to_hdf' : True}
                }
            }
        }

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
                for file in f:
                    if '.asc' in file:
                        if not 'lock' in file: # exlude locked files
                            if not '.txt' in file:
                                files.append(os.path.join(r, file))

        if len(files) > 0:
            print('OK, found %d asc files' % len(files))
            #for i in files:
            #    print(i)
        else:
            sys.exit('ERROR: No asc files found')

        return files

    ## @brief read asc into pandas dataframe
    #
    # @param args, dict: dictionary of arguments
    # @returns stacked_df: pandas dataframe
    def read_as_pandas(self, args):
        print('--> read asc into pandas dataframe')
        # placeholder
        df_all = {}
        u_pnames = []
        # read raklida area dataset
        try:
            raklida_area = h5py.File(args['src_folder'] + '/raklida_gis/raklida.h5', 'r')
            raklida_ds = np.flipud(np.array(raklida_area['Band1'])).flatten().astype(float)
            raklida_ds[raklida_ds<0] = np.nan
        except:
            sys.exit("ERROR: " + str(sys.exc_info()))
        # get unique parameters
        for f in args['asc_files']:
            pname, idparam = self.get_parameter_from_fname(args, f)
            if not pname in u_pnames:
                u_pnames.append(pname)
        print('>>>>> parameters', u_pnames)

        # loop over parameter names
        for pname in u_pnames:
            #if pname == 'TM0':
            # create a key in dict if not exists
            if not pname in df_all.keys():
                df_all[pname] = pd.DataFrame()
            df_raklida = pd.DataFrame()
            next_year = 2100
            # initialize df_stacked
            df_stacked = pd.DataFrame({'id':raklida_ds})
            df_stacked = df_stacked[df_stacked['id'].notna()]
            # read csv file
            args['asc_files'] = sorted(args['asc_files'])
            #print('--> asc_files: ', args)
            
            for f in args['asc_files']:
                if not '.xml' in f:
                    if pname in f:
                        print('reading by rasterio', f)
                        # get year and month from filename
                        year, month = self.get_year_month_from_fname(f)

                        print(year, next_year)

                        #handle missing years
                        if next_year < year:
                            next_year = year + 1
                            # reinitialize df_stacked
                            df_stacked = pd.DataFrame({'id':raklida_ds})
                            df_stacked = df_stacked[df_stacked['id'].notna()]
                            print('*****************', str(year))
                            print(f)

                        raster = rasterio.open(f).read(1).flatten() # read file by rasterio
                        # get nodata value
                        nodata = self.get_nodata_value_from_asc(f)
                        raster[raster == nodata] = np.nan
                        # loop over one year
                        if year <= next_year:
                            if year != next_year and month != 'dec':
                                # stack data into pandas dataframe
                                print(raklida_ds.shape)
                                print(raster.shape)
                                print(month)
                                
                                df = pd.DataFrame({'id':raklida_ds, 'value':raster}).rename(columns={'value':month})
                                df = df[df['id'].notna()]
                                df_stacked['year'] = year
                                df_stacked = pd.merge(df_stacked, df, how='left', on='id')
                                print(df_stacked.shape)
                            elif year != next_year and  month == 'dec':
                                # stack data of one year into pandas dataframe
                                df = pd.DataFrame({'id':raklida_ds, 'value':raster}).rename(columns={'value':month})
                                df = df[df['id'].notna()]
                                df_stacked['year'] = year
                                df_stacked = pd.merge(df_stacked, df, how='left', on='id')
                                # add df_stacked (one complete year) to df_all
                                frames = [df_raklida, df_stacked]
                                df_raklida = pd.concat(frames)
                                print(df_raklida.shape, df_stacked.shape)
                                print('df_raklida.shape: ', df_raklida.shape)
                                #sys.exit()
                                if df_raklida.shape[1] > 14:
                                    print(df_stacked)
                                    sys.exit('ERROR: shape not correct')
                            else:
                                # reinitialize df_stacked
                                df_stacked = pd.DataFrame({'id':raklida_ds})
                                df_stacked = df_stacked[df_stacked['id'].notna()]
                                df = pd.DataFrame({'id':raklida_ds, 'value':raster}).rename(columns={'value':month})
                                df = df[df['id'].notna()]
                                df_stacked['year'] = year
                                df_stacked = pd.merge(df_stacked, df, how='left', on='id')
                                print('##### ', str(year))
                                #break
                                #print(df_stacked)
                            next_year = year + 1

                        #if next_year == 2023:
                        #    break

                    #break
                df_all[pname] = df_raklida
                #break
                #print(df_all)
                #sys.exit()
        return df_all

    ## @brief Get nodata value
    #
    # @param file, string
    # @returns nodata, float
    def get_nodata_value_from_asc(self, file):
        print('--> get nodata value from file')
        nodata = None
        file = open(file, 'r')
        for line in file.readlines():
            if 'nodata_value' in line:
                nodata = float((line.split(' '))[2])
                break
        file.close()
        if nodata == None:
            sys.exit('ERROR: Missing nodata value in asc file', file)

        return nodata


    ## @brief Get year and month from filename
    #
    # @param filename: string
    # @returns year, month: integer
    def get_year_month_from_fname(self, file):
        print('--> get year and month from filename')
        year, month = None, None
        months = {1:'jan', 2:'feb', 3:'mar', 4:'apr', 5:'may', 6:'jun', 7:'jul', 8:'aug', 9:'sep', 10:'oct', 11:'nov', 12:'dec'}

        yearmonth = file.split('.')[0].split('_')[-1]
        year = int(yearmonth[0:4])

        if int(yearmonth[4:6]) in range(1,13):
            month = int(yearmonth[4:6])

        if year == None or month == None:
            sys.exit('ERROR: Missing year or month')

        return year, months[month]


    ## @brief Get parameter name and id from filename
    #
    # @param filename: string
    # @returns pname, id: integer
    def get_parameter_from_fname(self, args, file):
        
        #print ('---> : file :',file)
        #print ('---> : conf :',str(self.conf[args['data_type']]['parameters']))
        
        #print('--> get parameter name and id from filename')
        pname, idparam = None, None
        for p in self.conf[args['data_type']]['parameters']:
            if p in file:
                idparam = self.conf[args['data_type']]['parameters'][p]['id']
                pname = p

        if pname == None or idparam == None:
            sys.exit('ERROR: Missing parameter name or parameter id')

        return pname, idparam

    ## @brief Save parameters as hdf5
    #
    # @param df, pandas DataFrame
    def transform_as_hdf(self, args, dict):
        print('--> transform data to hdf5')
        print(dict.keys())
        # loop over each parameter
        for p in dict.keys():
            if self.conf[args['data_type']]['parameters'][p]['to_hdf'] == True:
                # get dataset for a parameter
                df = dict[p]
                print('parameter --> ', p)
                start_year = df['year'].min()
                last_year = df['year'].max()
                print('start year', start_year)
                print('last year', last_year)
                for y in range(int(start_year),int(last_year)+1):
                    print(y)
                    # get data for one year
                    df_year = df.loc[df['year'] == y]
                    # reorganize data to final structure: index, values_block_0[]
                    df_final = pd.DataFrame(columns=['index','values_block_0'])
                    df_final['index'] = df_year['id']
                    df_final['values_block_0'] = df_year[df_year.columns[2:]].apply(
                        lambda x: ' '.join(x.astype(str)),
                        axis=1
                    )
                    df_final = df_final.set_index('index')

                    print(df_final.dtypes)
                    # save as hdf5
                    path = self.args['dst_folder'] + '/' + str(self.args['scenario_id']) + '/' + str(self.conf[args['data_type']]['parameters'][p]['id']) + '/'
                    if not os.path.exists(path):
                        os.makedirs(path)
                    fn = str(self.args['scenario_id']) + '_' + str(self.conf[args['data_type']]['parameters'][p]['id']) + '_' + str(y) + '.h5'
                    print(path + fn)
                    #sys.exit()
                    try:
                        df_final[['values_block_0']].to_hdf(path + fn, key='table' , mode='w', format='table', complevel=9)
                    except:
                        sys.exit("ERROR: " + str(sys.exc_info()))

    
    ## @brief create derived parameters and store them into a pandas dataframe
    #
    # @param args, dict: dictionary of arguments
    # @returns stacked_df: pandas dataframe
    def create_derived_parameters(self, args):
        print('--> Create derived parameters')
        df = {}
        # loop over derived parameters to create
        for p in self.conf[args['data_type']]['parameters']:
            if p == 'pi_summer' and self.conf[args['data_type']]['parameters'][p]['to_hdf'] == True:
                print('+++')
                df['pi_summer'] = self.derive_pi_summer(args)

            if p == 'pi_winter' and self.conf[args['data_type']]['parameters'][p]['to_hdf'] == True:
                print('+++')
                df['pi_winter'] = self.derive_pi_winter(args)

        if len(df) == 0:
            sys.exit('ERROR: dataframe creation failed')

        return df
    
    
    ## @brief Derive pi_summer
    #
    # sum of parameters 12, 13, 25, 26, 27, 28, 29
    def derive_pi_summer(self, args):
        print('--> Derive pi_summer')
        ds = {}
        # loop over files
        src = args['src_folder'] + '/' + str(args['scenario_id']) + '/10/'
        print('--> src', src)
        for r, d, f in os.walk(src):
            for file in f:
                df = pd.DataFrame()
                # get year from fname
                year = file.split('.')[0].split('_')[-1]
                print(year)
                 
                try:
                     # read parameters for one year# read the h5 file
                    df_10 = pd.read_hdf(args['src_folder'] + '/' + str(args['scenario_id']) + '/10/' + str(args['scenario_id']) + '_10_' + str(year) + '.h5', key='table').reset_index()
                    # set integer type for column, set nan values
                    df_10['index'] = df_10['index'].astype('float').replace(-99999, np.nan)
                    # get a column for each month
                    df_10[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']] = df_10.values_block_0.str.split(expand=True,).astype(float).replace(-99999, np.nan)
                    # remove column values_block_0
                    df_10 = df_10.drop(columns=['values_block_0'])
                    # create sum of flows
                    decimals = self.conf[args['data_type']]['parameters']['pi_summer']['decimals']
                    df['index'] = df_10['index']
                    df['jan'] = 0
                    df['feb'] = 0
                    df['mar'] = 0
                    df['apr'] = df_10['apr'].round(decimals)
                    df['may'] = df_10['may'].round(decimals)
                    df['jun'] = df_10['jun'].round(decimals)
                    df['jul'] = df_10['jul'].round(decimals)
                    df['aug'] = df_10['aug'].round(decimals)
                    df['sep'] = df_10['sep'].round(decimals)
                    df['oct'] = 0
                    df['nov'] = 0
                    df['dec'] = 0
                    ds[str(year)] = df
                except:
                    print("ERROR: ")

                    
                

        return ds


    ## @brief Derive pi_winter
    #
    # sum of parameters 12, 13, 25, 26, 27, 28, 29
    def derive_pi_winter(self, args):
        print('--> Derive pi_winter')
        ds = {}
        # loop over files
        src = args['src_folder'] + '/' + str(args['scenario_id']) + '/10/'
        print('--> src', src)
        for r, d, f in os.walk(src):
            for file in f:
                df = pd.DataFrame()
                # get year from fname
                year = file.split('.')[0].split('_')[-1]
                print(year)
               
                try:
                    # read parameters for one year# read the h5 file
                    df_10 = pd.read_hdf(args['src_folder'] + '/' + str(args['scenario_id']) + '/10/' + str(args['scenario_id']) + '_10_' + str(year) + '.h5', key='table').reset_index()
                    # set integer type for column, set nan values
                    df_10['index'] = df_10['index'].astype('float').replace(-99999, np.nan)
                    # get a column for each month
                    df_10[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']] = df_10.values_block_0.str.split(expand=True,).astype(float).replace(-99999, np.nan)
                    # remove column values_block_0
                    df_10 = df_10.drop(columns=['values_block_0'])
                    # create sum of flows
                    decimals = self.conf[args['data_type']]['parameters']['pi_winter']['decimals']
                    df['index'] = df_10['index']
                    df['jan'] = df_10['jan'].round(decimals)
                    df['feb'] = df_10['feb'].round(decimals)
                    df['mar'] = df_10['mar'].round(decimals)
                    df['apr'] = 0
                    df['may'] = 0
                    df['jun'] = 0
                    df['jul'] = 0
                    df['aug'] = 0
                    df['sep'] = 0
                    df['oct'] = df_10['oct'].round(decimals)
                    df['nov'] = df_10['nov'].round(decimals)
                    df['dec'] = df_10['dec'].round(decimals)
                    # assign dataframe to dictionary
                    ds[str(year)] = df
                except:
                    print("ERROR: ")

        return ds

    ## @brief Save derived parameters
    def save_derived_parameters(self, args, df):
        print('--> Save derived parameters')
        for p in df:
            print('===>>', p)
            if df[p] != None:
                for y in df[p]:
                    print(y)
                    df_year = df[p][str(y)]
                    # reorganize data to final structure: index, values_block_0[]
                    df_final = pd.DataFrame(columns=['index','values_block_0'])
                    df_final['index'] = df_year['index']
                    df_final['values_block_0'] = df_year[df_year.columns[1:]].apply(
                        lambda x: ' '.join(x.astype(str)),
                        axis=1
                    )
                    df_final = df_final.set_index('index')
                    #print(df_final)
                    print(df_final.dtypes)
                    # save as hdf5
                    path = self.args['dst_folder'] + str(self.args['scenario_id']) + '/' + str(self.conf[args['data_type']]['parameters'][p]['id']) + '/'
                    if not os.path.exists(path):
                        os.makedirs(path)
                    fn = str(self.args['scenario_id']) + '_' + str(self.conf[args['data_type']]['parameters'][p]['id']) + '_' + str(y) + '.h5'
                    print(path + fn)
                    #sys.exit()
                    try:
                        df_final[['values_block_0']].to_hdf(path + fn, key='table' , mode='w', format='table')
                    except:
                        sys.exit("ERROR: " + str(sys.exc_info()))
