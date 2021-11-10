import sys, os
import simplejson
import time
sys.path.append('/mnt/visdat/Projekte/2020/GWN viewer/dev/python/extract_data')
from extract_data_cube import extract_data_cube
print(os.path.exists('/mnt/visdat/Projekte/2020//GWN viewer/dev/python/extract_data/'))


#python3 /mnt/visdat/Projekte/2020/GWN\ viewer/dev/python/extract_data/main_cube.py


defs = {}
defs['param'] = {}
defs['param']['time'] = {}
defs['project'] = {}
defs['param']['id_param'] = 10
defs['param']['id_scenario'] = 4
defs['param']['id_area'] = 0
defs['param']['id_areadata'] = [0]
defs['param']['time_period'] = 'year'
#defs['param']['group_area'] = 1
defs['param']['time']['start_year'] = 1988
#defs['param']['time']['start_month'] = 0
defs['param']['time']['end_year'] = 2014
#defs['param']['time']['end_month'] = 0
defs['project']['proj_dir'] = "/mnt/galfdaten/daten_stb/gwn_sachsen/"

# start program
t0 = time.time()
print("--> starting parameter extraction...")
p = extract_data_cube()

# get command line arguments
args = p.get_arguments(defs, 'test')
print('--> args : ' + str(args))
print('--> args_proj_dir : ' + str(args['proj_dir']))

# check area file
args['area_fpath'] = p.check_area_file(args)

# check parameter file
args['parameter_fpath'] = p.check_parameter_file(args)

# extract kliwes dataset
parameter, area, years = p.extract_parameter(args)

# get data for mapping by mapserver live
#result = p.get_map_data(args, parameter, area)

print('--> args : ' + str(args))

# save mapping data 
#args['parameter_fpath_diff'] = '/mnt/galfdaten/daten_stb/gwn_sachsen/parameters/0/33/nc/33_0_1_1962_1999.int32.2.h5'
#args['parameter_fpath'] = '/mnt/galfdaten/daten_stb/gwn_sachsen/parameters/0/10/nc/10_0_1_1962_1999.int32.2.h5'
#args['param_export_fpath'] = '/mnt/galfdaten/daten_stb/gwn_sachsen/parameters/0/20/nc/20_0_1_1962_1999.int32.2.h5'

#args['exists'], args['param_export_fpath'] = p.check_parameter_export_file(args)

#args['param_export_fpath'] = '/mnt/galfdaten/daten_stb/gwn_sachsen/parameters/0/20/nc/20_0_1_1962_1999.int32.2.h5'
#args['parameter_fpath_quotient'], args['parameter_fpath_divisor'] = p.check_parameters_abw_file(args)
#print('--> args : ' + str(args))
#p.create_map_data_abw(args)

args['exists'], args['param_export_fpath'] = p.check_parameter_export_file(args)


print('--> args : ' + str(args))
p.create_map_data(args)


# todo: get base statistics
#result = p.get_base_statistic(args, parameter, area)
#print(result)

# todo :  get timeline
#result = p.get_timeline(args, parameter, area, years)

# todo : get histogram:#
#esult = p.get_histogram(args, parameter, area)

# get data for mapping by mapserver
#result = p.get_area_values(args, parameter, area)

#print(result)
