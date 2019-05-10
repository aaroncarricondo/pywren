'''
Created on 8 may. 2019

@author: aaroni34
'''

import random, yaml, pika
import pywren_ibm_cloud as pywren


def my_map_function(ide):
    num = random.randint(0,1000)
    return num



#Load COS, Functions and RabbitAMQP information
with open('ibm_cloud_config', 'r') as config_file:
    try:
        
        res = yaml.safe_load(config_file)
        
    except yaml.YAMLError as exc:
        
        print(exc)
        
#Get URL
rabbit = res['ibm_rabbit']

# Declare connection and new queue on RABBITAMQ
params = pika.URLParameters(str(rabbit))
connection = pika.BlockingConnection(params)
channel = connection.channel() # start a channel

#----------------------
#----- QUEUE ------
channel.queue_delete('queue')
channel.queue_declare(queue='queue')



pw = pywren.ibm_cf_executor(rabbitmq_monitor=True)


pw = pywren.ibm_cf_executor(rabbitmq_monitor=True)
pw.map(my_map_function, range(2))
pw.
pw.clean()