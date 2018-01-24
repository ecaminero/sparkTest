from pyspark import SparkContext
sc = SparkContext()
sc.setCheckpointDir('/user/cloudera')

import os
import numpy as np
# Test no Supervisado
# Crea un archivo del nombre indicado
fid = open('/user/cloudera/output_kmeans.txt','w')

# lectura de datos de ratings
iris_data = sc.textFile('/user/cloudera/iris.data')

# obtenemos los atributos del archivo, separamos por , los datos que estan registrados
iris_attrib_raw = iris_data.map(lambda line: np.array([l for l in line.split(",")[:-1]]))

# solo usamos los datos validos
iris_attrib = iris_attrib_raw.filter(lambda x: len(x)==4)

# obtenemos la clase, y la guardamos en el vector iris_data con c
iris_class = iris_data.map(lambda line: line.split(",")).filter(lambda x: len(x)==5).map(lambda l: l[4]).collect()

# Entreno k-means

# K: numero de clusters que encontrara kmeans
K = 3 # especifica el grupo de datos del set de entreamieno

from pyspark.mllib.clustering import KMeans
# Aplicando prubemas con los mismos datos de entrenamiento
model = KMeans.train(iris_attrib, k=K, maxIterations = 100)

# Guardo los centros de los clusters
centers = model.clusterCenters

fid.write('Centros de clusters:\n\n')
for c in centers:
	fid.write(str(c) + '\n')

# encuentro el label predicho y lo comparo con la clase real
labels = model.predict(iris_attrib).collect()
fid.write('\n\nLabels (real - predicted)\n\n')
for l,c in zip(labels, iris_class):
	fid.write(str(l) + ' - ' + str(c) + '\n')
fid.close()

