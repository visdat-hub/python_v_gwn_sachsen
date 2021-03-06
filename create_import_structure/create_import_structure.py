## @package Create import structure of *.h5 files similiar to the Wasserhaushaltsportal
#
# Read *.csv files and restructure the content
#
# - destination file: paramId_scenarioId.stats.h5 -src_folder
# - file system folder structure: /scenarioId/parameterId/*.h5
# - file name structure: scenarioId_parameterId_year.h5
# - h5 folder structure: /data/table
# - table structure: column names --> index, values_block_0
#   - index is starting with 0 (id_areadata)
#   - values_block_0 contains the monthly data as array: [ 7820  8380 14810  5470  3790  7009 11500  4460  7259  6210  4810  3590]
#
# Usage
# ----------
# cd /mnt/visdat/Projekte/2020/GWN Viewer/dev/python/create_import_structure/
#
# kliwes
# ------
# python3 create_import_structure.py  -src_folder /mnt/visdat/Projekte/2020/GWN\ viewer/daten/kliwes/ -dst_folder /mnt/visdat/Projekte/2020/GWN\ viewer/daten/restructured/kliwes -data_type kliwes -scenario_id 0
#
# kliwes_derived
# ------
# python3 create_import_structure.py  -src_folder /mnt/visdat/Projekte/2020/GWN\ viewer/daten/restructured/kliwes/ -dst_folder /mnt/visdat/Projekte/2020/GWN\ viewer/daten/restructured/kliwes/ -data_type kliwes_derived -scenario_id 0
#
# difga
# ------
# python3 create_import_structure.py  -src_folder /mnt/visdat/Projekte/2020/GWN\ viewer/datcd /mnt/visdat/Projekte/2020/GWN Viewer/dev/python/generate_data/en/difga/EZG\ DIFGA\ Zeitreihen\ 11-1960-10-2014/ -dst_folder /mnt/visdat/Projekte/2020/GWN\ viewer/daten/restructured/difga -data_type difga -scenario_id 1
#
# difga_derived
# ------
# python3 create_import_structure.py  -src_folder /mnt/visdat/Projekte/2020/GWN\ viewer/daten/restructured/difga/ -dst_folder /mnt/visdat/Projekte/2020/GWN\ viewer/daten/restructured/difga -data_type difga_derived -scenario_id 1
#
# raklida
# ------
# id_scenario -> raklida_messungen: 2, raklida_referenz: 3, raklida_wettreg66: 4
# python3 create_import_structure.py  -src_folder /mnt/visdat/Projekte/2020/GWN\ viewer/daten/Raklida\ Messungen/ -dst_folder /mnt/visdat/Projekte/2020/GWN\ viewer/daten/restructured/raklida_messungen -data_type raklida_messungen -scenario_id 2
#
# stoffbilanz
# ------
# id_scenario: 5
# python3 create_import_structure.py  -src_folder /mnt/visdat/Projekte/2020/GWN\ viewer/daten/stoffbilanz/ -dst_folder /mnt/visdat/Projekte/2020/GWN\ viewer/daten/restructured/stoffbilanz/ -data_type stoffbilanz -scenario_id 5
#
# Command line arguments
# ----------
# - src_folder : string, folder of csv data
# - dst_folder : string, destination folder for *.h5 files
# - data_type : string, kliwes, difga, raklida_messungen, raklida_referenz, raklida_wettreg66
# - scneario_id : integer, scenario identifier

import sys
import os
from kliwes import kliwes_import
from kliwes2 import kliwes2_import
from difga import difga_import
from difga2 import difga2_import
from raklida import raklida_import
from stoffbilanz import stoffbilanz_import

## @brief Get command line arguments.
#
# @returns args Array of arguments
def get_arguments():
    print("--> get caller arguments")
    args = {
        'src_folder' : None,
        'dst_folder' : None,
        'data_type' : None,
        'scenario_id' : None
    }

    i = 1
    while i < len(sys.argv):
        arg = sys.argv[i]
        if arg == '-src_folder':
            i = i + 1
            args['src_folder'] = sys.argv[i]
        elif arg == '-dst_folder':
            i = i + 1
            args['dst_folder'] = sys.argv[i]
        elif arg == '-data_type':
            i = i + 1
            args['data_type'] = sys.argv[i]
        elif arg == '-scenario_id':
            i = i + 1
            args['scenario_id'] = sys.argv[i]
        i = i + 1

    for p in args:
        if args[p] == None:
            print("ERROR: arguments of program call missing --> ", p)
            sys.exit()

    return args

## @brief Check if folder exists
#
# @param folder_list, list of folders to check
def check_folder(folder_list):
    print('--> check folders')
    for f in folder_list:
        if not os.path.exists(f):
            sys.exit('ERROR: folder not exists --> ' + f)
        else:
            print('OK, folder found --> ', f)


if __name__ ==  '__main__':
    """ start program """

    # get command line arguments
    args = get_arguments()

    # check if folder exists
    check_folder([args['src_folder'], args['dst_folder']])

    # kliwes data import
    if args['data_type'] == 'kliwes':
        kliwes = kliwes_import(args)
        # kliwes data: read csv files from zip files
        #args['csv_files'] = kliwes.read_csv(args['src_folder'])
        args['csv_files'] = kliwes.read_csv_by_scenario(args['src_folder'])
        # read csv into pandas dataframe
        #df = kliwes.read_as_pandas(args)
        
        #print('--> args   ', args)
        
        dict_df = kliwes.read_as_pandas_by_scenarios(args)
        #print(dict_df)
        #sys.exit()
        # save parameters as hdf5
        #kliwes.transform_as_hdf(df)
        kliwes.transform_as_hdf_by_scenario(dict_df)
        

    # difga data import
    if args['data_type'] == 'difga':
        difga = difga_import(args)
        # read dbase files
        args['dbf_files'] = difga.read_dbase(args['src_folder'])
        
        # read dbf into pandas dataframe
        df = difga.read_as_pandas(args)
        # save parameters as hdf5
        #difga.transform_as_hdf(df)

    # raklida data import
    if args['data_type'] == 'raklida_messungen' or args['data_type'] == 'raklida_referenz' or args['data_type'] == 'raklida_wettreg66':
        raklida = raklida_import(args)
        # read asc files
        args['asc_files'] = raklida.read_asc(args['src_folder'])
        #print('--> asc_files:', args['asc_files'])
        
        # read files into pandas dataframe
        df = raklida.read_as_pandas(args)
        #print('--> df:', df)
        # save parameters as hdf5
        raklida.transform_as_hdf(args, df)

    # kliwes derived data import
    if args['data_type'] == 'kliwes_derived':
        kliwes = kliwes_import(args)
        # create derived parameters
        df = kliwes.create_derived_parameters(args)
        # save data
        kliwes.save_derived_parameters(args, df)
        
    # kliwes derived data import
    if args['data_type'] == 'kliwes2_derived':
        kliwes2 = kliwes2_import(args)
        # create derived parameters
        df = kliwes2.create_derived_parameters(args)
        # save data
        kliwes2.save_derived_parameters(args, df)

    # difga derived data import
    if args['data_type'] == 'difga_derived':
        difga = difga_import(args)
        # create derived parameters
        df = difga.create_derived_parameters(args)
        # save data
        difga.save_derived_parameters(args, df)
        
    # difga derived data import
    if args['data_type'] == 'difga2_derived':
        difga = difga2_import(args)
        # create derived parameters
        df = difga.create_derived_parameters(args)
        # save data
        difga.save_derived_parameters(args, df)
    
        
    # raklida data import
    if args['data_type'] == 'raklida_messungen_derived' or args['data_type'] == 'raklida_referenz_derived' or args['data_type'] == 'raklida_wettreg66_derived':
        raklida = raklida_import(args)
        # create derived parameters
        df = raklida.create_derived_parameters(args)
        # save data
        raklida.save_derived_parameters(args, df)

    # stoffbilanz data import
    if args['data_type'] == 'stoffbilanz':
        stoffbilanz = stoffbilanz_import(args)
        # read shapefiles / dbf files
        args['dbf_files'] = stoffbilanz.read_shapefiles(args['src_folder'])
        # read dbase into a pandas DataFrame
        df = stoffbilanz.read_as_pandas(args)
        # save parameters as hdf5
        stoffbilanz.transform_as_hdf(args, df)
