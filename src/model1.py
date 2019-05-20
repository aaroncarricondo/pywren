'''
Created on 8 may. 2019

@author: aaroni34
'''

import random, yaml, pika, sys, os
import pywren_ibm_cloud as pywren
from pathlib import Path

iterdata = range(int(sys.argv[1]))

def my_master_function(n):
    
    #Initialize the array and counter for consuming writing desires
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
    #Declare queue
    channel.queue_declare(queue='master', auto_delete=True)
    channel.basic_consume(callback_master, queue='master')
    #Start consuming writers petitions (IDs)
    channel.start_consuming()
    
    num_writers = len(iterdata) - 1
    
    for i in range(0, len(iterdata)):
        
        num = random.randint(0,num_writers)
        
        #Take id that will write and remove it from the writers desire to write
        write = array[num]
        del array[num]
        
        channel.basic_publish(exchange='logs', routing_key='', body='W ' + write)
        num_writers = num_writers - 1
    
    
    connection.close()
    

def my_map_function(ide):
    
    array = []
    counter = 0
    
    def callback(channel, method, header, body):
        
        nonlocal counter 
        nonlocal array 
        
        channel.basic_ack(delivery_tag = method.delivery_tag)
        
        body = body.decode('utf-8')
        body_list = body.split()
        
        if (len(body_list) == 2):
            
            if (body_list[1] == str(ide)):
                num = random.randint(0,200)
                channel.basic_publish(exchange='logs', routing_key='', body=str(num))
                
        else:
            #Append random number to array and increment counter
            array.append(body)
            counter = counter + 1
            if (counter >= len(iterdata)): 
                channel.stop_consuming()
    
    #Create connection to RabbitAMQP
    params = pika.URLParameters(str(os.environ.get('rabbit')))
    connection = pika.BlockingConnection(params)
    channel = connection.channel() # start a channel
    #Generate random
    num = random.randint(0,200)
    
    #Publish want to write (ide) in the topic queue of the master
    channel.basic_publish(exchange='', routing_key='master', body= str(ide))
    
    #Associate callback_function to queue
    channel.basic_consume(callback, queue=str(ide))
    #Start consuming and processing values
    channel.start_consuming()
    
    #Close connection to the channel for auto-deleting queues
    connection.close()
    
    return array


#Load RabbitAMQP information
config_path=str(Path.home())+'/.pywren_config'

with open(config_path, 'r') as config_file:
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

#----------------------------------------------------------------
#----------------------- DELETE QUEUES --------------------------
print("Deleting previous queues . . .")
for i in range (0, 20):
    channel.queue_delete(queue=str(i))
    print("Deleted queue ", i)

#Declare the exchange queue (fanout) to share random numbers
channel.exchange_declare(exchange='logs', exchange_type='fanout')

#Declare queues for every map function and bind them to the fanout queue
print("Creating and binding map queues . . .")
for i in iterdata:
    channel.queue_declare(queue=str(i), auto_delete=True)
    channel.queue_bind(exchange='logs', queue=str(i))

#Execute functions
pw = pywren.ibm_cf_executor();
extra_env={'rabbit' : rabbit}
#Call asynchronous to master function
pw.call_async(func=my_master_function, data=3 , extra_env = extra_env)
#Execute map functions
pw.map(my_map_function, iterdata, extra_env = extra_env)
result = pw.get_result()

#Delete first element because is the output of the master
del result[0]

#Print results on a file
file = open("results.txt", "w")

for i in result:
    file.write(str(i)+ "\n")
file.close()



#Close connection to the channel for auto-deleting queues
connection.close()