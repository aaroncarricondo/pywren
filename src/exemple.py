'''
Created on 8 may. 2019

@author: aaroni34
'''

import random, yaml, pika, sys, os
import pywren_ibm_cloud as pywren

iterdata = range(int(sys.argv[1]))

def my_master_function():
    
    array = []
    counter = 0
    
    def callback_master(channel, method, header, body):
        
        nonlocal counter 
        nonlocal array 
        # We've received numDiv messages, stop consuming
        channel.basic_ack(delivery_tag = method.delivery_tag)
        
        #Append random number to array and increment counter
        array.append(body.decode('utf-8'))
        counter = counter + 1
        if (counter >= len(iterdata)): 
            channel.stop_consuming()
    
    
    params = pika.URLParameters(str(os.environ.get('rabbit')))
    connection = pika.BlockingConnection(params)
    channel = connection.channel() # start a channel
    
    channel.queue_declare(queue='master', auto_delete=True)
    
    channel.basic_consume(callback_master, queue='master')
    channel.start_consuming()
    
    num = random.randint(0,len(iterdata)) - 1
    
    write = array[num]
    
    channel.basic_publish(exchange='logs', routing_key='', body='W ' + write)
    
    
    
    connection.close()
    
    


def my_map_function(ide):
    
    array = []
    counter = 0
    
    def callback(channel, method, header, body):
        
        nonlocal counter 
        nonlocal array 
        # We've received numDiv messages, stop consuming
        channel.basic_ack(delivery_tag = method.delivery_tag)
        
        body = body.decode('utf-8')
        header, body = body.split(' ')
        
        if (header == 'W'):
            
            if (body == str(ide)):
                channel.basic_publish(exchange='logs', routing_key='', body=str(num))
                
        else:
            #Append random number to array and increment counter
            array.append(body)
            counter = counter + 1
            if (counter >= len(iterdata)): 
                channel.stop_consuming()
    
    params = pika.URLParameters(str(os.environ.get('rabbit')))
    connection = pika.BlockingConnection(params)
    channel = connection.channel() # start a channel
    #Generate random
    num = random.randint(0,200)
    
    #Publish want to write (ide) in the topic queue of the master
    channel.basic_publish(exchange='', routing_key='master', body= str(ide))
    
    #Associate
    channel.basic_consume(callback, queue=str(ide))
    #Start consuming and processing values
    channel.start_consuming()
    
    #Close connection to the channel for auto-deleting queues
    connection.close()
    
    return array


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


for i in iterdata:
    channel.queue_declare(queue=str(i), auto_delete=True)
    channel.queue_bind(exchange='logs', queue=str(i))

#Execute functions
pw = pywren.ibm_cf_executor();
extra_env={'rabbit' : rabbit}
pw.map(my_map_function, iterdata, extra_env = extra_env)
result = pw.get_result()

#Print results on a file
file = open("results.txt", "w")
for i in result:
    file.write(str(i)+ "\n")
file.close()

#Close connection to the channel for auto-deleting queues
connection.close()