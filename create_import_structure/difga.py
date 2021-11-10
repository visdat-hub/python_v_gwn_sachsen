## @package Read DIFGA time series from dbase files
import sys
import os
import h5py
import pandas as pd
import numpy as np
#from dbfread import DBF
import geopandas as gpd

class difga_import:
    def __init__(self, args):
        """ configure output """
        ## command line arguments
        self.args = args
        ## configuration of parameter properties
        self.conf = {
            'parameters' : {
                'P' : {'id':10, 'decimals':2, 'dtype':'int32', 'to_hdf' : True},
                'RG2' : {'id':13, 'decimals':2, 'dtype':'int32', 'to_hdf' : True},
                'RG1' : {'id':12, 'decimals':2, 'dtype':'int32', 'to_hdf' : True},
                'RD' : {'id':16, 'decimals':2, 'dtype':'int32', 'to_hdf' : True},
                'QG2' : {'id':15, 'decimals':2, 'dtype':'int32', 'to_hdf' : True},
                'QG1' : {'id':14, 'decimals':2, 'dtype':'int32', 'to_hdf' : True},
                'REST' : {'id':31, 'decimals':2, 'dtype':'int32', 'to_hdf' : True},
                'DEF' : {'id':18, 'decimals':2, 'dtype':'int32', 'to_hdf' : True}
            },
            'derived_parameters' : {
                'pi_summer' : {'id':6, 'decimals':2, 'dtype':'int32', 'to_hdf' : False},
                'pi_winter' : {'id':7, 'decimals':2, 'dtype':'int32', 'to_hdf' : False},
                'gwn' : {'id':32, 'decimals':2, 'dtype':'int32', 'to_hdf' : False},
                'sicker' : {'id':34, 'decimals':2, 'dtype':'int32', 'to_hdf' : True}
            }
        }

    ## @brief Read dbase files
    #
    # @param src, string : source folder
    # @return dbf_list, list: list of files
    def read_dbase(self, src):
        print('--> read dbase files')
        files = []

        # get KLIWES files
        if 'difga' in src:
            for r, d, f in os.walk(src):
                for file in f:
                    if '.dbf' in file:
                        if not 'lock' in file: # exlude locked files
                            files.append(os.path.join(r, file))

        if len(files) > 0:
            print('OK, found %d dbf files' % len(files))
            #for i in files:
            #    print(i)
        else:
            sys.exit('ERROR: No dbf files found')
        return files

    ## @brief read dbf into pandas dataframe
    #
    # @param args, dict: dictionary of arguments
    # @returns stacked_df: pandas dataframe
    def read_as_pandas(self, args):
        print('--> read dbf into pandas dataframe')

        # placeholder
        stacked_df = pd.DataFrame()
        # read csv file
        df_difga = pd.DataFrame()
        for f in args['dbf_files']:
            print('reading', f)
            # get name measuring point
            name = f.split('/')[-1].split('_')[0]
            print(name)
            table = gpd.read_file(f)
            table['pegelname'] = name
            df = pd.DataFrame(table.drop(columns='geometry'))
            df[['month','year']] = df['MONAT'].str.split('/',expand=True,)
            df['year'] = df['year'].astype(int)
            df['month'] = df['month'].astype(int)
            df_difga = pd.concat([df_difga, df])
            print(df_difga)

        # overall dataframe
        print("OK, read %d Megabytes " % (np.asarray(df_difga).nbytes / 1000000))

        return df_difga

    ## @brief Save parameters as hdf5
    #
    # @param df, pandas DataFrame
    def transform_as_hdf(self, df):
        print('--> transform data to hdf5')
        print(df.keys())
        # loop over each parameter
        for p in self.conf['parameters']:
            if self.conf['parameters'][p]['to_hdf'] == True:
                print('parameter --> ', p)
                start_year = df['year'].min()
                last_year = df['year'].max()
                print('start year', start_year)
                print('last year', last_year)
                for y in range(int(start_year),int(last_year)+1):
                    print(y)
                    # get data
                    df_param = df[df['year'] == y]
                    df_param = df_param[['pegelname','year','month', p]]
                    df_pivot = df_param.pivot(index='pegelname', columns='month', values=p)
                    df_pivot = df_pivot.reset_index()
                    # reorganize data with monthes as columns
                    df_year = pd.DataFrame(columns=['index','jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec'])
                    df_year['index'] = df_pivot['pegelname']
                    df_year['jan'] = df_pivot[1] if 1 in df_pivot else -9999
                    df_year['feb'] = df_pivot[2] if 2 in df_pivot else -9999
                    df_year['mar'] = df_pivot[3] if 3 in df_pivot else -9999
                    df_year['apr'] = df_pivot[4] if 4 in df_pivot else -9999
                    df_year['may'] = df_pivot[5] if 5 in df_pivot else -9999
                    df_year['jun'] = df_pivot[6] if 6 in df_pivot else -9999
                    df_year['jul'] = df_pivot[7] if 7 in df_pivot else -9999
                    df_year['aug'] = df_pivot[8] if 8 in df_pivot else -9999
                    df_year['sep'] = df_pivot[9] if 9 in df_pivot else -9999
                    df_year['oct'] = df_pivot[10] if 10 in df_pivot else -9999
                    df_year['nov'] = df_pivot[11] if 11 in df_pivot else -9999
                    df_year['dec'] = df_pivot[12] if 12 in df_pivot else -9999
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
                    path = self.args['dst_folder'] + '/' + str(self.args['scenario_id']) + '/' + str(self.conf['parameters'][p]['id']) + '/'
                    if not os.path.exists(path):
                        os.makedirs(path)
                    fn = str(self.args['scenario_id']) + '_' + str(self.conf['parameters'][p]['id']) + '_' + str(y) + '.h5'
                    print(path + fn)
                    try:
                        df_final[['values_block_0']].to_hdf(path + fn, key='table' , mode='w', format='table')
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
        for p in self.conf['derived_parameters']:
            
            if p == 'pi_summer' and self.conf['derived_parameters'][p]['to_hdf'] == True:
                print('+++')
                df['pi_summer'] = self.derive_pi_summer(args)

            if p == 'pi_winter' and self.conf['derived_parameters'][p]['to_hdf'] == True:
                print('+++')
                df['pi_winter'] = self.derive_pi_winter(args)
            
            if p == 'gwn' and self.conf['derived_parameters'][p]['to_hdf'] == True:
                print('+++')
                df['gwn'] = self.derive_gwn(args)
                
            if p == 'sicker' and self.conf['derived_parameters'][p]['to_hdf'] == True:
                print('+++')
                df['sicker'] = self.derive_sicker(args)
                
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
                # read parameters for one year# read the h5 file
                df_10 = pd.read_hdf(args['src_folder'] + '/' + str(args['scenario_id']) + '/10/' + str(args['scenario_id']) + '_10_' + str(year) + '.h5', key='table').reset_index()
                # set integer type for column, set nan values
                #df_10['index'] = df_10['index'].astype('float').replace(-9999, np.nan)
                df_10['index'] = df_10['index'].replace(-9999, np.nan)
                # get a column for each month
                df_10[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']] = df_10.values_block_0.str.split(expand=True,).astype(float).replace(-9999, np.nan)
                # remove column values_block_0
                df_10 = df_10.drop(columns=['values_block_0'])
                # create sum of flows
                decimals = self.conf['derived_parameters']['pi_summer']['decimals']
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
                # assign dataframe to dictionary
                ds[str(year)] = df
                
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
                # read parameters for one year# read the h5 file
                df_10 = pd.read_hdf(args['src_folder'] + '/' + str(args['scenario_id']) + '/10/' + str(args['scenario_id']) + '_10_' + str(year) + '.h5', key='table').reset_index()
                # set integer type for column, set nan values
                #df_10['index'] = df_10['index'].astype('float').replace(-9999, np.nan)
                df_10['index'] = df_10['index'].replace(-9999, np.nan)
                # get a column for each month
                df_10[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']] = df_10.values_block_0.str.split(expand=True,).astype(float).replace(-9999, np.nan)
                # remove column values_block_0
                df_10 = df_10.drop(columns=['values_block_0'])
                # create sum of flows
                decimals = self.conf['derived_parameters']['pi_winter']['decimals']
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
                
        return ds
    
    ## @brief derive groundwater recharge (gwn)
    def derive_gwn(self, args):
        print('--> derive groundwater recharge')
        ds = {}
        # loop over rg2 files
        src = args['src_folder'] + '/' + str(args['scenario_id']) + '/13/'
        for r, d, f in os.walk(src):
            for file in f:
                # get year from fname
                year = file.split('.')[0].split('_')[-1]
                print(year)
                # read parameters 13 for one year# read the h5 file
                df_rg2 = pd.read_hdf(args['src_folder'] + '/' + str(args['scenario_id']) + '/13/' + str(args['scenario_id']) + '_13_' + str(year) + '.h5', key='table').reset_index()
                df_rg2['index'] = df_rg2['index'].replace(-9999, np.nan)
                df_rg2[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']] = df_rg2.values_block_0.str.split(expand=True,).astype(float).replace(-9999, np.nan)
                df_rg2 = df_rg2.drop(columns=['values_block_0'])
                #print(df_rg2)
                # read parameters 12 for one year# read the h5 file
                df_rg1 = pd.read_hdf(args['src_folder'] + '/' + str(args['scenario_id']) + '/12/' + str(args['scenario_id']) + '_12_' + str(year) + '.h5', key='table').reset_index()
                df_rg1['index'] = df_rg1['index'].replace(-9999, np.nan)
                df_rg1[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']] = df_rg1.values_block_0.str.split(expand=True,).astype(float).replace(-9999, np.nan)
                df_rg1 = df_rg1.drop(columns=['values_block_0'])
                #print(df_rg1)
                # read locker_fest
                df_difga_lockerfest = pd.read_csv('/mnt/visdat/Projekte/2020/GWN viewer/daten/difga/difga_locker_fest.csv', sep=';').sort_values(by=['id_org'])
                print('--> df_difga_lockerfest read...OK ' + df_difga_lockerfest.columns)
                #print(df_difga_lockerfest)
                # summarize df12 and df13
                df_merge = pd.merge(df_rg2, df_rg1, how='left', on='index')
                print(df_merge.columns)
                df_sum = pd.DataFrame()
                df_sum['index'] = df_merge['index']
                df_sum['jan'] = df_merge['jan_x'] + df_merge['jan_y']
                df_sum['feb'] = df_merge['feb_x'] + df_merge['feb_y']
                df_sum['mar'] = df_merge['mar_x'] + df_merge['mar_y']
                df_sum['apr'] = df_merge['apr_x'] + df_merge['apr_y']
                df_sum['may'] = df_merge['may_x'] + df_merge['may_y']
                df_sum['jun'] = df_merge['jun_x'] + df_merge['jun_y']
                df_sum['jul'] = df_merge['jul_x'] + df_merge['jul_y']
                df_sum['aug'] = df_merge['aug_x'] + df_merge['aug_y']
                df_sum['sep'] = df_merge['sep_x'] + df_merge['sep_y']
                df_sum['oct'] = df_merge['oct_x'] + df_merge['oct_y']
                df_sum['nov'] = df_merge['nov_x'] + df_merge['nov_y']
                df_sum['dec'] = df_merge['dec_x'] + df_merge['dec_y']
                #print(df_sum)
                # merge df_sum with locker_fest
                df_locker = pd.merge(df_difga_lockerfest, df_sum, how='inner', left_on='id_org', right_on='index')
                df_locker = df_locker.loc[df_locker['locker_fest']==0]
                #print(df_locker)
                # merge df_rg2 with locker_fest
                df_fest = pd.merge(df_difga_lockerfest, df_rg2, how='inner', left_on='id_org', right_on='index')
                df_fest = df_fest.loc[df_fest['locker_fest']==1]
                #print(df_fest)
                print('df_locker ' + str(df_locker.shape))
                print('df_fest ' + str(df_fest.shape))
                
                # stack datasets
                df_gwn = pd.concat([df_locker, df_fest]).drop(columns=['idarea', 'idarea_data', 'gkz', 'id_org', 'locker_fest'])
                print('df_gwn' + str(df_gwn.shape) + str(df_gwn.columns)) 
                #print(df_gwn)
                ds[str(year)] = df_gwn
                #break
        
        #sys.exit()
        return ds
    
    ## @brief derive  sicker
    def derive_sicker(self, args):
        print('--> derive groundwater recharge')
        ds = {}
        # loop over rg2 files
        src = args['src_folder'] + '/' + str(args['scenario_id']) + '/13/'
        for r, d, f in os.walk(src):
            for file in f:
                # get year from fname
                year = file.split('.')[0].split('_')[-1]
                print(year)
                # read parameters 13 for one year# read the h5 file
                df_rg2 = pd.read_hdf(args['src_folder'] + '/' + str(args['scenario_id']) + '/13/' + str(args['scenario_id']) + '_13_' + str(year) + '.h5', key='table').reset_index()
                df_rg2['index'] = df_rg2['index'].replace(-9999, np.nan)
                df_rg2[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']] = df_rg2.values_block_0.str.split(expand=True,).astype(float).replace(-9999, np.nan)
                df_rg2 = df_rg2.drop(columns=['values_block_0'])
                #print(df_rg2)
                # read parameters 12 for one year# read the h5 file
                df_rg1 = pd.read_hdf(args['src_folder'] + '/' + str(args['scenario_id']) + '/12/' + str(args['scenario_id']) + '_12_' + str(year) + '.h5', key='table').reset_index()
                df_rg1['index'] = df_rg1['index'].replace(-9999, np.nan)
                df_rg1[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']] = df_rg1.values_block_0.str.split(expand=True,).astype(float).replace(-9999, np.nan)
                df_rg1 = df_rg1.drop(columns=['values_block_0'])
                #print(df_rg1)
                # summarize df12 and df13
                df_merge = pd.merge(df_rg2, df_rg1, how='left', on='index')
                print(df_merge.columns)
                df = pd.DataFrame()
                df['index'] = df_merge['index']
                df['jan'] = df_merge['jan_x'] + df_merge['jan_y']
                df['feb'] = df_merge['feb_x'] + df_merge['feb_y']
                df['mar'] = df_merge['mar_x'] + df_merge['mar_y']
                df['apr'] = df_merge['apr_x'] + df_merge['apr_y']
                df['may'] = df_merge['may_x'] + df_merge['may_y']
                df['jun'] = df_merge['jun_x'] + df_merge['jun_y']
                df['jul'] = df_merge['jul_x'] + df_merge['jul_y']
                df['aug'] = df_merge['aug_x'] + df_merge['aug_y']
                df['sep'] = df_merge['sep_x'] + df_merge['sep_y']
                df['oct'] = df_merge['oct_x'] + df_merge['oct_y']
                df['nov'] = df_merge['nov_x'] + df_merge['nov_y']
                df['dec'] = df_merge['dec_x'] + df_merge['dec_y']
                #print(df)
                
                ds[str(year)] = df
                #break
        
        #sys.exit()
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
                    path = self.args['dst_folder'] + '/' + str(self.args['scenario_id']) + '/' + str(self.conf['derived_parameters'][p]['id']) + '/'
                    if not os.path.exists(path):
                        os.makedirs(path)
                    fn = str(self.args['scenario_id']) + '_' + str(self.conf['derived_parameters'][p]['id']) + '_' + str(y) + '.h5'
                    print(path + fn)
                    #sys.exit()
                    try:
                        df_final[['values_block_0']].to_hdf(path + fn, key='table' , mode='w', format='table')
                    except:
                        sys.exit("ERROR: " + str(sys.exc_info()))
            else:
                print('ERROR: empty dataframe')
