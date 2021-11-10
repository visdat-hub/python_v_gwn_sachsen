## @package Generator for application data of GWN viewer
#
# Usage
# ----------
# cd /mnt/visdat/Projekte/2020/GWN Viewer/dev/python/generate_data/
#
# kliwes
# -------
# python3 generate_data.py  -src_folder /mnt/visdat/Projekte/2020/GWN\ viewer/daten/restructured/kliwes/0/ -dst_folder /var/rosi_data/daten_stb/gwn_sachsen/ -data_type kliwes -scenario_id 0 -level_id 1 -area_id 12 -max_year 1961
#
# difga
# -------
# python3 generate_data.py  -src_folder /mnt/visdat/Projekte/2020/GWN\ viewer/daten/restructured/difga/ -dst_folder /var/rosi_data/daten_stb/gwn_sachsen/ -data_type difga -scenario_id 1 -level_id 1 -area_id 12 -max_year 1961
#
# raklida_messungen
# -------
# python3 generate_data.py  -src_folder /mnt/visdat/Projekte/2020/GWN\ viewer/daten/Raklida\ Messungen/ -dst_folder /var/rosi_data/daten_stb/gwn_sachsen/ -data_type raklida_messungen -scenario_id 2 -level_id 1 -area_id 12 -max_year 1961
#
# raklida_referenz
# -------
# python3 generate_data.py  -src_folder /mnt/visdat/Projekte/2020/GWN\ viewer/daten/Raklida\ Referenz/ -dst_folder /var/rosi_data/daten_stb/gwn_sachsen/ -data_type raklida_referenz -scenario_id 2 -level_id 1 -area_id 12 -max_year 1961
#
# raklida_wettreg66
# -------
# python3 generate_data.py  -src_folder /mnt/visdat/Projekte/2020/GWN\ viewer/daten/Raklida\ Wettreg\ 66/ -dst_folder /var/rosi_data/daten_stb/gwn_sachsen/ -data_type raklida_wettreg66 -scenario_id 2 -level_id 1 -area_id 12 -max_year 1961
#
# stoffbilanz
# -------
# python3 generate_data.py  -src_folder /mnt/visdat/Projekte/2020/GWN\ viewer/daten/restructured/stoffbilanz/ -dst_folder /var/rosi_data/daten_stb/gwn_sachsen/ -data_type stoffbilanz -scenario_id 5 -level_id 1 -area_id 12 -max_year 1961
#
# Command line arguments
# ----------
# - src_folder : string, folder of h5 parameter data generated by create_import_structure
# - dst_folder : string, destination folder for *.h5 files and folder of difga area data (netcdf)
# - data_type : string, kliwes, difga, raklida_messungen, raklida_referenz, raklida_wettreg66
# - scneario_id : integer, scenario identifier
# - level_id : integer, level identifier to set spatial resolution
#   - level 1 : 100m
#   - level 2 : 50m
#   - level 3 : 25m
# - area_id : integers, area identifier of kliwes dataset
# - max_year: integer, limit count of years to import for test purposes

import sys
import os
from kliwes import kliwes_generator
from kliwes2_25 import kliwes2_25_generator
from kliwes2_100 import kliwes2_100_generator
from difga import difga_generator
from raklida import raklida_generator
from stoffbilanz import stoffbilanz_generator

## @brief Get command line arguments.
#
# @returns args Array of arguments
def get_arguments():
    print("--> get caller arguments")
    args = {
        'src_folder' : None,
        'dst_folder' : None,
        'data_type' : None,
        'scenario_id' : None,
        'level_id' : None,
        'area_id' : None,
        'max_year' : None
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
        elif arg == '-level_id':
            i = i + 1
            args['level_id'] = sys.argv[i]
        elif arg == '-area_id':
            i = i + 1
            args['area_id'] = sys.argv[i]
        elif arg == '-max_year':
            i = i + 1
            args['max_year'] = sys.argv[i]
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

def unlink_maps(self, phath):
    for folder, subfolder, file in os.walk(phath):
        for filename in file:
            if filename.endswith(".int32.2.h5"):

                print(folder,filename)
                os.unlink(os.path.join(folder,filename))

## @brief Example program to generate data for the GWN Viewer
if __name__ ==  '__main__':
    """ start program """

    # get command line arguments
    args = get_arguments()

    # check if folder exists
    check_folder([args['src_folder'], args['dst_folder']])

    #unlink_maps(args['dst_folder'])

    # kliwes data import
    if args['data_type'] == 'kliwes':
        kliwes = kliwes_generator(args)
        # get areas
        area_ds, x, y = kliwes.get_area()
        # create parameter datasets
        #kliwes.create_parameter(area_ds)
        # create yearly parameter datasets for all scenarios
        kliwes.create_parameter_by_scenarios(area_ds, x, y)
        
    # kliwes data import
    if args['data_type'] == 'kliwes2_25':
        
        kliwes = kliwes2_25_generator(args)
        # get areas
        area_ds, x, y = kliwes.get_area()
        # create parameter datasets
        #kliwes.create_parameter(area_ds)
        # create yearly parameter datasets for all scenarios
        kliwes.create_parameter_by_scenarios(area_ds, x, y)
        
    # kliwes data import
    if args['data_type'] == 'kliwes2_100':
        
        kliwes = kliwes2_100_generator(args)
        # get areas
        area_ds, x, y = kliwes.get_area()
        # create parameter datasets
        #kliwes.create_parameter(area_ds)
        # create yearly parameter datasets for all scenarios
        kliwes.create_parameter_by_scenarios(area_ds, x, y)

    # difga data import
    if args['data_type'] == 'difga':
        difga = difga_generator(args)
        # prepare area dataset for overlapping difga areas
        area_ds, param_config = difga.prepare_area_data(args)
        # for each parameter defined in param_config
        # uncomment lines in difga.py in self.conf['parameters']
        for p in param_config['parameters']:
            print('--> Parameter', p)
            # create and save parameter dataset with corrected data for intersected difga levels
            param_ds = difga.prepare_parameter_data(args, param_config['parameters'][p], area_ds)

    # raklida data import
    if args['data_type'] == 'raklida_messungen':
        raklida = raklida_generator(args)
        params = []
        years_range = []
        # get filenames
        args['asc_files'] = raklida.read_asc(args['src_folder'])
        # get parameters to generate
        params = raklida.get_unique_parameters_from_folder(args)
        # get min year, max year
        years_range = raklida.get_years_range(args)
        # create parameter datasets
        raklida.create_raklida_messungen(args, years_range, params)

    if args['data_type'] == 'raklida_referenz':
        raklida = raklida_generator(args)
        params = []
        years_range = []
        # get filenames
        args['asc_files'] = raklida.read_asc(args['src_folder'])
        # get parameters to generate
        params = raklida.get_unique_parameters_from_folder(args)
        if len(params) > 0:
            # get min year, max year
            years_range = raklida.get_years_range(args)
            # create parameter datasets
            raklida.create_raklida_referenz(args, years_range, params)
        else:
            print('!!! Creation of cubes for RRK and TM0 not initiated')

        if raklida.conf['raklida_referenz']['SN']['to_hdf'] == True:
            raklida.create_raklida_referenz_sn_wn(args, 'SN')
        if raklida.conf['raklida_referenz']['WN']['to_hdf'] == True:
            raklida.create_raklida_referenz_sn_wn(args, 'WN')


    if args['data_type'] == 'raklida_wettreg66':
        raklida = raklida_generator(args)
        params = []
        years_range = []
        # get filenames
        args['asc_files'] = raklida.read_asc(args['src_folder'])
        # get parameters to generate
        
        print('param : ', params)
        #sys.exit()
        params = raklida.get_unique_parameters_from_folder(args)
        if len(params) > 0:
            #print('huhu')
            print(params)
            # get min year, max year
            years_range = raklida.get_years_range(args)
            # create parameter datasets
            raklida.create_raklida_wettreg66(args, years_range, params)
        else:
            print('!!! Creation of cubes for RRK and TM0 not initiated')

        if raklida.conf['raklida_wettreg66']['SN']['to_hdf'] == True:
            raklida.create_raklida_wettreg66_sn_wn(args, 'SN')
        if raklida.conf['raklida_wettreg66']['WN']['to_hdf'] == True:
            raklida.create_raklida_wettreg66_sn_wn(args, 'WN')

    if args['data_type'] == 'stoffbilanz':
        stoffbilanz = stoffbilanz_generator(args)
        # get areas
        area_ds, x, y = stoffbilanz.get_area()
        # create parameter datasets
        stoffbilanz.create_parameter(area_ds, x, y)