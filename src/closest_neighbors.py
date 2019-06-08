import numpy as np
import math

DATA_LOCATION = "../data"
FILE = "mote_locs.txt"
locations_file = "{}/{}".format(DATA_LOCATION, FILE)

sensors_loc = []

with open(locations_file, "r") as f:
	lines = f.readlines()
	for line in lines:
		info = line.split(" ")
		sensor = int(info[0])
		x = float(info[1])
		y = float(info[2])
		sensors_loc.append([sensor, x, y])

sensors_loc = np.array(sensors_loc)


def getClosestNeighbors(sensorId, sensors_loc):
	"""
	returns a list of neighbors ordered from closest to furthest to the given sensorId
	"""

	index_sensor_id = np.where(sensors_loc[:,0] == sensorId)[0][0]
	x_sensor = sensors_loc[index_sensor_id, 1]
	y_sensor = sensors_loc[index_sensor_id, 2]

	neighbors = []
	distances = []
	for i in range(len(sensors_loc)):
		if i!= index_sensor_id:
			id_neighbor = sensors_loc[i,0]
			x_neighbor = sensors_loc[i,1]
			y_neighbor = sensors_loc[i,2]
			x = x_sensor - x_neighbor
			y = y_sensor - y_neighbor
			distance = math.sqrt(math.pow(x,2) + math.pow(y,2))
			neighbors.append(id_neighbor)
			distances.append(distance)

	ar_neighbors = np.array(neighbors)
	ar_distances = np.array(distances)
	inds = ar_distances.argsort()
	sorted_neighbors = ar_neighbors[inds]
	sorted_distances = ar_distances[inds]

	return sorted_neighbors, sorted_distances



sorted_neighbors, sorted_distances = getClosestNeighbors(24, sensors_loc)

print("sorted neighbors")
print(sorted_neighbors)
print("sorted distances")
print(sorted_distances)

