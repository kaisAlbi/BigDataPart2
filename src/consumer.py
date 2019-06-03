import time
import re, ast
import numpy as np
import os

from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils



def getKafkaDStream(spark,topic='temperature',batch_interval=10):

	#Get Spark context
	sc=spark.sparkContext

	#Create streaming context, with required batch interval
	ssc = StreamingContext(sc, batch_interval)

	#Checkpointing needed for stateful transforms
	ssc.checkpoint("checkpoint")
    
	#Create a DStream that represents streaming data from Kafka, for the required topic 
	dstream = KafkaUtils.createStream(ssc, "127.0.0.1:2181", "spark-streaming-consumer", {topic: 1})
    
	return [sc,ssc,dstream]


def parseRow(row):
    '''parses a single row into a dictionary'''

    try:
        v = row.split(" ")
        return [{"sensor_type": int(v[0]),
                 "time": datetime.strptime(v[1] + " "+ v[2], "%Y-%m-%d %H:%M:%S.%f"),
                 "p-i": v[3],
                 "measurement": round(float(v[4]),1),       #rounded precision to 1 digit (follow the requirements)
                 "voltage": float(v[5])}]
    except Exception as err:
        print("Unexpected error: %s" % (err))


def updateFunction(new_values, state): 
	model = state[0]
	if model == "persistence":

		last_temperature=state[1]
		sensorToPredict=state[2]
    
		if len(new_values)>0 :
    	#Transforms list of values into an array
			array_values=np.array(new_values)
			print(array_values)
			#if day 8
			if np.floor(array_values[0][1] / 86400)==7:
	            
				predictions=[]
				truth=[]
				seconds=[]
	            
				#Go through all measurements
				for i in range(0,array_values.shape[0]):
	                
					if array_values[i,2]==sensorToPredict:
						#With persistence, the model is the last temperature for sensor 1 observed in the last batch
						predictions.append(last_temperature)
	                    
						truth.append(array_values[i,0])
						seconds.append(array_values[i,1])
	                
					#else:
						#Possibly adapt model. Add code here to adapt model if you use masurements from other sensors.
	                    
				#Store data in state
				output_day8=[predictions,truth,seconds]
	            
			else:
				if array_values[0][1] % 86400<8:
					#Before day 8, adapt your model with measurements of the current batch
					#For persistence model this is simply keeping the last measurement for sensor 1
					#Note: Below is a code for a more general case, for illustration purposes, 
					#where there may be sensors other than sensor 1, and a loop other all input values may be necessary
					for i in range(0,array_values.shape[0]):
	                
						if array_values[i,2]==sensorToPredict:
							current_temperature=np.float(array_values[i,0])
	                        
					last_temperature=current_temperature
	        
		#Update state
		state=["persistence", last_temperature,sensorToPredict]
        
	#state is now the last received measurement
	return state

"""
##TODO change update function so that it applies our 3 learning algorithms
def updateFunction(new_values, state): 
	## RLS update function
    ## Only update with first value of RDD. You should transofrm new_values to array, and update models for all values 
    if (len(new_values)>0 ):
        
        key=new_values[0][0]
        yx=new_values[0][1]
        i=yx[0]
        y=yx[1]
        x=yx[2:]
        n=len(x)
        
        beta=state[1]
        beta.shape=(n,1)
        V=state[2]
        mu=state[3]
        sse=state[4]  ## sum of squared errors
        N=state[5]   ## number of treated samples
        x.shape=(1,n)
        err=y-x.dot(beta)
        sse=sse+pow(err,2.0)
        V=1.0/mu*(V-V.dot(x.T).dot(x).dot(V)/(1.0+float(x.dot(V).dot(x.T))))
        gamma=V.dot(x.T)
        beta=beta+gamma*err
        
        return (key,beta,V,mu,sse/(N+1.0),N+1)  ## update formula mod1
        
    else:
        return state
"""


os.environ['PYSPARK_SUBMIT_ARGS'] = '--conf spark.ui.port=4040 '+\
								'--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.1 '+\
								'--conf spark.driver.memory=2g  pyspark-shell'

spark = SparkSession \
	.builder \
	.master("local[3]") \
	.appName("KafkaReceive") \
	.getOrCreate()


#This function creates a connection to a Kafka stream
#You may change the topic, or batch interval
#The Zookeeper server is assumed to be running at 127.0.0.1:2181
#The function returns the Spark context, Spark streaming context, and DStream object



"""
seconds_per_day = 86400
slots_per_day = 2880
# RLS INIT VAR
# init weight, covariance matrix and forgetting parameter
nb_features = 5
forgetting_factor = 1.0
betas = np.zeros(nb_features)
covar_matrix = np.diag(np.zeros(nb_features)+1)

state1=('mod1',beta1,V1,mu1,0,0)
"""

#PERSISTENCE model
last_measurement=0
#Batch interval (to be synchronized with KafkaSend)
interval=30
state_1 = ["persistence", last_measurement,1]	#Last measurement: prediction & sensor to predict
state_24 = ["persistence", last_measurement,24]
#state_pers_1=('persistence',beta2,V2,mu2,0,0)

[sc,ssc,dstream]=getKafkaDStream(spark=spark,topic='temperature',batch_interval=1)

dstream.pprint()
#dstream=dstream.flatMap(lambda x: [('mod1',('mod1',1.0*np.array(x))),
#                            ('mod2',('mod2',1.0*np.array(x)))])

#initialStateRDD = sc.parallelize([state_1, state_24])

#dstream=dstream.updateStateByKey(updateFunction,initialRDD=initialStateRDD)


#For synchronization with receiver (for the sake of the simulation), starts at a number of seconds multiple of five
current_time=time.time()
time_to_wait=interval-current_time%interval
time.sleep(time_to_wait)

ssc.start()
ssc.awaitTermination()

