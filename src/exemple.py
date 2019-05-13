'''
Created on 8 may. 2019

@author: aaroni34
'''

import random, yaml, pika, sys, os
import pywren_ibm_cloud as pywren

iterdata = range(int(sys.argv[1]))



        


def my_map_function(ide):
    
    array=[]
    messages=0
    
    def callback(channel, method, header, body): 
        # We've received numDiv messages, stop consuming
        global messages
        global array 
        channel.basic_ack(delivery_tag = method.delivery_tag)
        array.append(body)
        messages += 1
        if messages >= len(iterdata):
            channel.stop_consuming()
    
    if (ide == 0):
        im_boss = True
    else:
        im_boss = False
        
    if (im_boss):
        waiting = []
    
    
    params = pika.URLParameters(str(os.environ.get('rabbit')))
    connection = pika.BlockingConnection(params)
    channel = connection.channel() # start a channel
    #channel.exchange_declare(exchange='logs', exchange_type='fanout')
    
   
    channel.queue_declare(str(ide))
    
    #Define function when consuming
    channel.basic_consume(callback, queue=str(ide))
    
    #Bind queue to exchange
    channel.queue_bind(exchange='logs', queue=str(ide))
    
    num = random.randint(0,50)
    channel.basic_publish(exchange='logs', routing_key='', body=str(num))
    
    channel.start_consuming()
    
    
    return iterdata






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
channel.exchange_declare(exchange='logs', exchange_type='fanout')



#----------------------
#----- QUEUE ------
#channel.queue_delete('queue')
#channel.queue_declare(queue='queue')


pw = pywren.ibm_cf_executor();
extra_env={'rabbit' : rabbit}
pw.map(my_map_function, iterdata, extra_env = extra_env)
result = pw.get_result()
print(result)



print("TODO OK")