from kafka import KafkaProducer
import time
import numpy as np

import pandas as pd


producer = KafkaProducer(bootstrap_servers='localhost:9092')


#Loop for sending messages to Kafka with the topic dataLinearModel

DATA_LOCATION = "../data"
FILE = "data.conv.txt"
SLEEP = 0.5
data_file = "{}/{}".format(DATA_LOCATION, FILE)

def convertTimeToSlots(dataframe):
    """
    Add a column 'slot' to the dataframe and divides the relative time in slots
    Also replace the 'seconds' value by the amount of seconds at the center of the slot
    :param dataframe: dataframe on which the operation are performed
    :return: modified dataframe
    """
    interval_slot = 30
    #divide data in slots of 30sec, add each slot value to each entry
    dataframe["slot"] = (dataframe["seconds"]//interval_slot).astype(int)
    #transform the seconds so that for each slot, its corresponding 'seconds' value is at the center of this slot (usefull for plots)
    dataframe["seconds"] = interval_slot*(dataframe["slot"] + dataframe["slot"]+1) / 2
    
    #Take care of the potential multiple value appearing within the same slot -> average them
    dataframe = dataframe.groupby(["slot"]).agg("mean")
    dataframe.reset_index(level=0, inplace=True)
    return dataframe

def fillMissingRows(dataframe):
    """
    Fill missing rows of the dataframe to ensure that there is a value at each time step (slot)
    so a prediction and a correction can be performed
    :param dataframe: dataframe on which the operation are performed
    :return: completed dataframe
    """
    interval_slot = 30
    nb_slots = 23040
    sensor_type = dataframe["SensorId"].values[0]
    missing_values = {"slot": [], "seconds": [], "SensorId": [], "Type": []}
    for i in range (nb_slots): #total nb of slots for 8 days
        if i not in dataframe["slot"].values:
            seconds = interval_slot*(i + i+1) / 2
            missing_values["slot"].append(i)
            missing_values["seconds"].append(seconds)
            missing_values["SensorId"].append(sensor_type)
            missing_values["Type"].append(0)
    #Build DataFrame with missing values
    temp_missing = pd.DataFrame(missing_values)
    #Merge the two Dataframe and sort them by values of the 'slot' column
    #At this point, the temperature values are still missing -> NaN
    complete_temp = dataframe.append(temp_missing).sort_values('slot')
    #Replace NaN by values extracted from a linear method based on the neighbors
    complete_temp["Value"] = complete_temp["Value"].interpolate(limit_direction="both")    
    return complete_temp

def preprocessDataFrame(dataframe):
    """
    Preprocess the different dataframes to add their time slots and their missing values
    :param output_df: dataframe on which  the operation are performed
    :return: complete dataframe
    """
    slots_per_day = 2880
    complete_df = fillMissingRows(convertTimeToSlots(dataframe))
    complete_df.reset_index(level=0, inplace=True)
    complete_df["slot"] = complete_df["slot"]%slots_per_day
    return complete_df

#Takes about one minute to load
data=pd.read_csv(data_file,header=None,sep=" ")
data.columns=["Date","Hour","Sensor","Value","Voltage"]
data=data.sort_values(['Date','Hour']).reset_index(drop=True)

data['datetime']=pd.to_datetime(data.Date+' '+data.Hour)
data['relative_datetime']=data['datetime']-data['datetime'][0]
data['seconds']=data['relative_datetime'].dt.total_seconds()

sensorId_type=data.Sensor.str.split("-",expand=True)
sensorId_type.columns=['SensorId','Type']

data['SensorId']=sensorId_type['SensorId'].astype(int)
data['Type']=sensorId_type['Type'].astype(int)
#Drop features not needed for the simulation
data = data.drop(['datetime','relative_datetime','Sensor','Date','Hour','Voltage'],axis=1)
#Retrieve temperature sensors 1 and 24, as well as their 5 respective closest neighbors
temp = data[((data.SensorId==1) | (data.SensorId==24) | (data.SensorId==2) | (data.SensorId==3) | (data.SensorId==33) | (data.SensorId==34) | (data.SensorId==35)\
	 		| (data.SensorId==22) |(data.SensorId==23) | (data.SensorId==25) | (data.SensorId==26) | (data.SensorId==27))\
			& (data.Type==0) & (data.seconds<=8*86400)]
print("temp retrieved")

sorted_temp = temp.reset_index(drop=True)

complete_df = preprocessDataFrame(sorted_temp)

interval=45

#Start at relative day 0 (2017-02-28)
day=0

#For synchronization with receiver (for the sake of the simulation), starts at a number of seconds multiple of 'interval'
current_time=time.time()
time_to_wait=interval-current_time%interval
time.sleep(time_to_wait)

#Loop for sending messages to Kafka with the topic persistence
for day in range(0,8):
    
    time_start=time.time()
    
    #Select sensor measurements for the corresponding relative day
    data_current_day=complete_df[(complete_df.seconds>=day*86400) & (complete_df.seconds<(day+1)*86400)]
    data_current_day=data_current_day.dropna()
    #For all measurements in that hour
    for i in range(len(data_current_day)):
        #Get data
        current_data=list(data_current_day.iloc[i])
        #Transform list to string
        message=str(current_data)
        #Send
        producer.send('temperature',message.encode())
    
    time_to_send=time.time()-time_start
    print("Time to send "+str(len(data_current_day))+" measurements (day "+str(day)+" ) : "+str(time_to_send))

    day=day+1
    
    time.sleep(interval-time_to_send)
