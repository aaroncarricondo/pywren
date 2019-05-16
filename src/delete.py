'''
Created on 16 may. 2019

@author: aaroni34
'''

import yaml, pika

#Load RabbitAMQP information
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

for i in range (0, 100):
    channel.queue_delete(queue=str(i))
