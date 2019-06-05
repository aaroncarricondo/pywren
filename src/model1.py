'''
Created on 8 may. 2019

@author: aaroni34
'''

import random, yaml, pika, sys, os, time
import pywren_ibm_cloud as pywren
from pathlib import Path

iterdata = range(int(sys.argv[1]))

def my_master_function(n):
    
    #Initialize the array and counter for consuming writing desires
    array = []
    counter = 0
    num_writers = len(iterdata)
    
    def callback_master(channel, method, header, body):
            
            nonlocal counter 
            nonlocal array 
            nonlocal num_writers
            # We've received numDiv messages, stop consuming
            channel.basic_ack(delivery_tag = method.delivery_tag)
            #Append random number to array and increment counter
            array.append(body.decode('utf-8'))
            counter = counter + 1
            if (counter > num_writers): 
                channel.stop_consuming()
                
    
    params = pika.URLParameters(str(os.environ.get('rabbit')))
    connection = pika.BlockingConnection(params)
    channel = connection.channel() # start a channel
    #Declare queue
    channel.queue_declare(queue='master', auto_delete=True)
    channel.basic_consume(callback_master, queue='master')
    
    
    while (num_writers > 0):
    
        #Start consuming writers petitions (IDs)    
        channel.start_consuming()
    
        #for _ in range(0, len(iterdata)):
        
        num = random.randint(0,num_writers-1)
        
        #Take id that will write and remove it from the writers desire to write
        write = array[num]
        del array[num]
        print("Sending write permission to "+ str(write) + " function . . .")
        
        channel.basic_publish(exchange='logs', routing_key='', body='W ' + write)
        num_writers = num_writers - 1
    
    connection.close()
    

def my_map_function(ide):
    
    array = []
    counter = 0
    writed = False
    
    def callback(channel, method, header, body):
        
        nonlocal counter 
        nonlocal array
        nonlocal writed
        
        channel.basic_ack(delivery_tag = method.delivery_tag)
        
        body = body.decode('utf-8')
        body_list = body.split()
        
        if (len(body_list) == 2):
            
            if (body_list[1] == str(ide)):
                num = random.randint(0,200)
                print("FunctionID" + str(ide) + " writing random number = " + str(num))
                channel.basic_publish(exchange='logs', routing_key='', body=str(num))
                writed = True
                
        else:
            #Append random number to array and increment counter
            print("Function " + str(ide) + "reading  number '" + str(body) + "' in position " + str(len(array)))
            array.append(body)
            counter = counter + 1
            if (counter >= len(iterdata)): 
                channel.stop_consuming()
            if (writed == False):
                #Publish want to write (ide) in the topic queue of the master
                channel.basic_publish(exchange='', routing_key='master', body= str(ide))
    
    #Create connection to RabbitAMQP
    params = pika.URLParameters(str(os.environ.get('rabbit')))
    connection = pika.BlockingConnection(params)
    channel = connection.channel() # start a channel
    
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
pw.call_async(func=my_master_function, data=3 , extra_env = extra_env, timeout=100)
#Execute map functions
pw.map(my_map_function, iterdata, extra_env = extra_env, timeout=100)
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