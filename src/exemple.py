'''
Created on 8 may. 2019

@author: aaroni34
'''

import random
import pywren_ibm_cloud as pywren


def my_map_function(ide):
    num = random.randint(0,1000)
    return num

pw = pywren.ibm_cf_executor(rabbitmq_monitor=True)


pw = pywren.ibm_cf_executor(rabbitmq_monitor=True)
pw.map(my_map_function, range(2))
pw.monitor()
pw.create_timeline_plots('/Users/nicolaydv/Desktop', 'rabbitmq')
pw.clean()