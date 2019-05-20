This is a publisher suscriber simple model where a master function decides who is going to write in a exchange FanOut queue, where the information written is distributed to all the other functions binded to it.

**A model example diagram:**

![alt text](https://i.ibb.co/R4n7B6P/Model1.png)

**Execution:**

  1.  First you need to have pywren correctly set up and have your configuration file with all the URLs in the path ~/.pywren_config 
      
      You can find how to do it in the following repository https://github.com/pywren/pywren-ibm-cloud
  
  2.  Execute the file model1.py :
  
      ```
      python3 model1.py (number_of_functions)
      ```
