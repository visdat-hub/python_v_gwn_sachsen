## @package Read kliwes datasets
import sys
import os
import pandas as pd
import numpy as np

class kliwes2_import:
    def __init__(self, args):
        """ configure output """
        ## command line arguments
        self.args = args
        ## configuration of parameter properties
        self.conf = {
            'derived_parameters' : {
                'pi_summer' : {'id':6, 'decimals':2, 'dtype':'int32', 'to_hdf' : False},
                'pi_winter' : {'id':7, 'decimals':2, 'dtype':'int32', 'to_hdf' : False}
            },
            'scenarios': {
                'ist' : {'id' : 30}
            }
        }

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
            
        if len(df) == 0:
            sys.exit('ERROR: dataframe creation failed')

        return df
    

    ## @brief Derive pi_summer
    #
    # 
    def derive_pi_summer(self, args):
        print('--> Derive pi_summer')
        ds = {}
        # loop over files
        src = args['src_folder'] + '/' + str(args['scenario_id']) + '/1/'
        print('--> src', src)
        for r, d, f in os.walk(src):
            for file in f:
                df = pd.DataFrame()
                # get year from fname
                year = file.split('.')[0].split('_')[-1]
                print(year)
                # read parameters for one year# read the h5 file
                df_10 = pd.read_hdf(args['src_folder'] + '/' + str(args['scenario_id']) + '/1/' + str(args['scenario_id']) + '_1_' + str(year) + '.h5', key='data').reset_index()
                # set integer type for column, set nan values
                df_10['index'] = df_10['index'].astype('float').replace(-9999, np.nan)
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
    # 
    def derive_pi_winter(self, args):
        print('--> Derive pi_winter')
        ds = {}
        # loop over files
        src = args['src_folder'] + '/' + str(args['scenario_id']) + '/1/'
        print('--> src', src)
        for r, d, f in os.walk(src):
            for file in f:
                df = pd.DataFrame()
                # get year from fname
                year = file.split('.')[0].split('_')[-1]
                print(year)
                # read parameters for one year# read the h5 file
                df_10 = pd.read_hdf(args['src_folder'] + '/' + str(args['scenario_id']) + '/1/' + str(args['scenario_id']) + '_1_' + str(year) + '.h5', key='data').reset_index()
                # set integer type for column, set nan values
                df_10['index'] = df_10['index'].astype('float').replace(-9999, np.nan)
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
    

    ## @brief Save derived parameters
    def save_derived_parameters(self, args, df):
        print('--> Save derived parameters')
        for p in df:
            print('===>>', p)
            if df[p] != None:
                for y in df[p]:
                    #print(y)
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
                    #print(df_final.dtypes)
                    # save as hdf5
                    path = self.args['dst_folder'] + str(self.args['scenario_id']) + '/' + str(self.conf['derived_parameters'][p]['id']) + '/'
                    if not os.path.exists(path):
                        os.makedirs(path)
                    fn = str(self.args['scenario_id']) + '_' + str(self.conf['derived_parameters'][p]['id']) + '_' + str(y) + '.h5'
                    #print(path + fn)
                    #sys.exit()
                    try:
                        df_final[['values_block_0']].to_hdf(path + fn, key='data' , mode='w', complevel=9, format='table')
                    except:
                        sys.exit("ERROR: " + str(sys.exc_info()))
