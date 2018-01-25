
# coding: utf-8
# # Parte I: Uso de funciones comunes de Spark
import time
from pyspark import SparkContext

t1 = time.time()
sc = SparkContext()

def generateData(dataJoined):
  return dataJoined.sortBy(lambda x: x[1][0], ascending=False).collect()

def joinData(genedateData, dataset):
  return genedateData.join(dataset)
# Extraemos la información o el dataset que esta en hdfs para realizar las consultas
# Generamos los RDD's de movies y ratings
movieText = sc.textFile('/user/cloudera/lab-spark-2/movies.dat')
ratingText = sc.textFile('/user/cloudera/lab-spark-2/ratings.dat')

# Generamos una Cache de los RDD para obtener velocidad
movieText.cache()
ratingText.cache()

# Hacemos un split de los datos que estan en el RDD ratingText para obtener la información
#  user_id, movie_id,  raiting,   timestamp
# [[u'1',   u'1193',    u'5',     u'978300760']]
ratingsData = ratingText.map(lambda line: line.split(';'))

# Generamos un RDD con el ID de la pelicula y 1 como primer voto
#      movie_id, rating
# Ej [(u'1193',    1)]
pairsRating = ratingsData.map(lambda vec: (vec[1], 1))

# agrupamos por movie_id y sumamos los ratings de cada uno
# (key, [value, value])
# Generamos un nuevo RDD con (movie_id, total_raitings)
countRating = pairsRating.reduceByKey(lambda a,b: a+b)

# Hacemos un split de los datos que estan en el RDD movieText para obtener la información
movieData = movieText.map(lambda line: line.split(';'))

# Generamos un RDD con la lista de los ids de las peliculas con sus nombres
# (movie_id, title)
pairsMovie = movieData.map(lambda vec: (vec[0], vec[1]))

# Ordenamos las peliculas de forma descendente
sortedData = countRating.sortBy(lambda x: x[1], ascending = False)

# Generamos una partición en el dataset con las 20 mejores peliculas, generalmente entre 2-4 particiones en cada CPU
bestTwenty = sc.parallelize(sortedData.take(20))
bestHundred = sc.parallelize(sortedData.take(100))

# Unimos las mejores peliculas mediante el movie_id
# Genemos un RDD con movie_id, raiting, title
joinedDataTwenty = joinData(bestTwenty, pairsMovie)
joinedDataHundred = joinData(bestHundred, pairsMovie)
# Ordenamos e imprimimos joinedData ordenado de forma descendente

bestTwenty = generateData(joinedDataTwenty)
bestHundred = generateData(joinedDataHundred)
# El valor generado es
# (movie_id, (countRating, title)

print 'El tiempo de ejecución es ' + str(time.time() - t1) + ' segundos'

# El tiempo de ejecución es 3.43885302544 segundos