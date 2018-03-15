
# coding: utf-8
# # Parte I: Uso de funciones comunes de Spark
import time
from pyspark import SparkContext

sc = SparkContext()

def generateData(dataJoined):
  return dataJoined.sortBy(lambda x: x[1][0], ascending=False).collect()

def joinData(genedateData, dataset):
  return genedateData.join(dataset)

def getBestRaiting(raiting):
  return raiting.filter(lambda element: element[1]>=100)

def ordering(data, ascending):
  return data.sortBy(lambda x: x[1], ascending = ascending)

def averageData(a, b):
  print(a, b)
  return a+b


# Extraemos la información del dataset que esta en hdfs para realizar las consultas
# Generamos los RDD's de movies y ratings
movieRDD = sc.textFile('/user/cloudera/lab-spark-2/movies.dat')
ratingRDD = sc.textFile('/user/cloudera/lab-spark-2/ratings.dat')

# Generamos una Cache de los RDD para obtener velocidad
# movieRDD.cache()
# ratingRDD.cache()

# Hacemos un split de los datos que estan en el RDD ratingRDD para obtener la información
#  user_id, movie_id,  raiting,   timestamp
# [[u'1',   u'1193',    u'5',     u'978300760']]
ratingsData = ratingRDD.map(lambda line: line.split(';'))

# Generamos un RDD con el ID de la pelicula y el raiting
#      movie_id, rating
# Ej [(u'1193',    1)]
pairsRating = ratingsData.map(lambda vec: (vec[1], 1))

# agrupamos por movie_id y sumamos los ratings de cada uno
# Ej: (key, [value, value])
# Generamos un nuevo RDD con (movie_id, total_raitings)
countRating = pairsRating.reduceByKey(lambda a,b: a+b)

# Hacemos un split de los datos que estan en el movieRDD para obtener la información
movieData = movieRDD.map(lambda line: line.split(';'))

# Generamos un RDD con la lista de los ids de las peliculas con sus nombres
# Ej: (movie_id, title)
pairsMovie = movieData.map(lambda vec: (vec[0], vec[1]))

# Ordenamos las peliculas de forma descendente para obtener las  mejores puntuaciones
sortedData = ordering(countRating, False)

# Generamos una partición en el dataset con las 20 mejores peliculas,
# generalmente entre 2-4 particiones en cada CPU
best = sc.parallelize(sortedData.take(20))
joinedDataTwenty = joinData(best, pairsMovie)
joinedData = ordering(joinedDataTwenty, False)

# -----------------------------------------------------------------------------------------------
# b) Muestre cuáles son las 20 películas con mayor cantidad de ratings.
# Mencione el tiempo en segundos que demora esta operación.
t1 = time.time()
joinedData.collect()
print 'El tiempo de ejecución es %s segundos' % (str(time.time()-t1))
# El tiempo de ejecución es 0.312572002411 segundos

# -----------------------------------------------------------------------------------------------
# d) Usando la función filter de los RDD de Spark, obtenga sólo las películas con 100 o más ratings.
bestRaiting = getBestRaiting(sortedData)
filterBestRaiting = sc.parallelize(bestRaiting.take(bestRaiting.count()))
joinedBestRaiting = joinData(filterBestRaiting, pairsMovie)

# -> i ¿Cuál es el número total de las películas, y cuánto es el número de las películas filtradas?
joinedBestRaiting.count()

# -> ii ¿Cuáles son las 10 películas con menor cantidad de ratings de las películas filtradas?
worstRaiting = ordering(bestRaiting, True)
worstRaiting = sc.parallelize(worstRaiting.take(10))
joinedWorstRaiting = joinData(worstRaiting, pairsMovie)
joinedWorstRaiting.collect()

# -----------------------------------------------------------------------------------------------
# e) Almacene en cache el RDD countRating, mediante la instrucción countRating.cache().
# Vuelva a repetir el análisis de b), contando el tiempo en que demora la operación.
best.cache()
joinedDataTwenty = joinData(best, pairsMovie)
joinedData = ordering(joinedDataTwenty, False)

t1 = time.time()
joinedData.collect()
print 'El tiempo de ejecución es %s segundos' % (str(time.time()-t1))
# El tiempo de ejecución es 0.271433115005 segundos
# -----------------------------------------------------------------------------------------------

# f) Efectúe el mismo análisis anterior entre b) y d), pero esta vez calculando el
# rating promedio de cada película,
#
averagePairsRating = ratingsData.map(lambda vec: (vec[1], int(vec[2])))
# ¿Qué operaciones debe efectuar después de pairsRatings para que se calcule el promedio de los ratings?
averageData = averagePairsRating.groupByKey().mapValues(lambda x: sum(x) / len(x))
bestAverageRaiting = ordering(averageData, False)

# b) Muestre cuáles son las 20 películas con mayor cantidad de ratings.
# Mencione el tiempo en segundos que demora esta operación.
t = time.time()
bestAverageRaiting.take(20)
print 'El tiempo de ejecución es %s segundos' % (str(time.time()-t))
# El tiempo de ejecución es 0.159805059433 segundos

# d) Usando la función filter de los RDD de Spark, obtenga sólo las películas con 100 o más ratings.
hundredAverageRaiting = getBestRaiting(bestAverageRaiting)
bestMovie = ordering(hundredAverageRaiting, True)
ratingsData.count() # total de las peluculas
bestMovie.count() # peliculas filtradas