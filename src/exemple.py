'''
Created on 8 may. 2019

@author: aaroni34
'''

import random, yaml, pika, sys, os, time
import pywren_ibm_cloud as pywren

iterdata = range(int(sys.argv[1]))


lista = []
contador = 0

def my_map_function(ide):
    
    
    
    def callback(channel, method, header, body):
        global lista
        global contador
        # We've received numDiv messages, stop consuming
        channel.basic_ack(delivery_tag = method.delivery_tag)
        lista.append(body.decode('utf-8'))
        contador = contador + 1
        if (contador >= len(iterdata)): 
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
    
    num = random.randint(0,200)
    channel.basic_publish(exchange='logs', routing_key='', body=str(num))
    
    channel.basic_consume(callback, queue=str(ide))
    
    #time.sleep(5)
    
    channel.start_consuming()
    
    
    return lista


    #Define function when consuming
    #channel.basic_consume(callback, queue=str(ide))
    
    #Bind queue to exchange
    #channel.queue_bind(exchange='logs', queue=str(ide))



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
for i in iterdata:
    channel.queue_delete(str(i))
    channel.queue_declare(queue=str(i))
    channel.queue_bind(exchange='logs', queue=str(i))
    


pw = pywren.ibm_cf_executor();
extra_env={'rabbit' : rabbit}
pw.map(my_map_function, iterdata, extra_env = extra_env)
result = pw.get_result()

file = open("salidaxd.txt", "w")

for i in result:
    file.write(str(i)+ "\n")

file.close()


print("TODO OK")