import sys, os
import simplejson
import time
sys.path.append('/mnt/visdat/Projekte/2020/GWN viewer/dev/python/extract_data')

from extract_data_area import extract_data_area
print(os.path.exists('/mnt/visdat/Projekte/2020/GWN viewer/dev/python/extract_data/'))


#python3 /mnt/visdat/Projekte/2020/GWN\ viewer/dev/python/extract_data/main_area.py

defs = {}
defs['param'] = {}
defs['param']['time'] = {}
defs['param']['id_param'] = 10
defs['param']['id_scenario'] = 10
defs['param']['id_area'] = 1
defs['param']['id_areadata'] = [0]
defs['param']['time_period'] = 'year'
defs['param']['time']['start_year'] = 1988
defs['param']['time']['start_month'] = 1
defs['param']['time']['end_year'] = 2014
defs['param']['time']['end_month'] = 12

defs['diffparam'] = {}
defs['diffparam']['time'] = {}
defs['diffparam']['id_scenario'] = 10
defs['diffparam']['time']['start_year'] = 1961
defs['diffparam']['time']['end_year'] = 1987

defs['project'] = {}
defs['project']['proj_dir'] =  "/mnt/galfdaten/daten_stb/gwn_sachsen/"
defs['project']['idtab'] =  406
defs['project']['python_dir'] =  "/mnt/visdat/Projekte/2020/GWN viewer/dev/python/extract_data"

defs['project']['id'] =  1000
defs['project']['diff'] =  1


# start program
p = extract_data_area()

# get command line arguments
args = p.get_arguments(defs, 'huhu')
print('--> args : ' + str(args))

if args['error'] == 0:
    # get pandas datasets reduced by id_areadata and time_period
    result = None
    dataset = p.get_data(args)
    #print('--> dataset : ' + str(dataset))

    # select extraction procedure
    #result = p.get_base_statistic_year(dataset, args)
    #result = p.get_base_statistic_month(dataset, args)
    #result = p.get_timeline_year(dataset, args)
    #result = p.get_timeline_month(dataset, args)
    #result = p.get_timeline_avg_month(dataset, args)
    #result = p.get_area_values(dataset, args)
    #result = p.get_avg_months_per_area_summer(dataset, args)
    #result = p.get_avg_months_per_area_winter(dataset, args)
    #result = p.save_h5data_to_csv(dataset, args)
    
    print(result)

    # calculate frequency distribution for difference datasets
    
    if args['diff'] == 1:
        diff_dataset = p.get_diff_data(args)
        print('--> diff_dataset : ' + str(diff_dataset))
        result = p.get_diff_histogram(args, dataset, diff_dataset)
    if args['diff'] == 0:
        result = p.get_histogram(args, dataset)
    
    print('--> result : ' + str(result))
    
