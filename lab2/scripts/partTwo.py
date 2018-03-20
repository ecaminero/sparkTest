
# coding: utf-8
# # Parte II: Uso de funciones de SparkSQL
# Crearemos tablas de SparkSQL a partir de los datos. Use los siguientes comandos para crear los Dataframes M y R:

# In[ ]:

from pyspark.sql import SQLContext, Row
from pyspark.sql.functions import asc, desc
import time
sqlContext = SQLContext(sc)

movieText = sc.textFile('/user/cloudera/lab-spark-2/movies.dat')
ratingText = sc.textFile('/user/cloudera/lab-spark-2/ratings.dat')

ratingsData = ratingText.map(lambda line: line.split(';'))
movieData = movieText.map(lambda line: line.split(';'))

movies = movieData.map(lambda m: Row(movie_id=int(m[0]), title=m[1], genres=m[2]))
M = sqlContext.createDataFrame(movies)

ratings = ratingsData.map(lambda r: Row(user_id=int(r[0]), movie_id=int(r[1]), rating=int(r[2]), timestamp=r[3]))
R = sqlContext.createDataFrame(ratings)

# ====== Respuestas
# El comando R.count() entregará el número de ratings, y lo mismo con el Dataframes M (movies).
# f) Encontraremos las 20 películas con mayor número de ratings, esta vez usando funciones de SparkSQL.
# Ejecute los siguientes comandos:
t1 = time.time()
MR = R.join(M, R.movie_id == M.movie_id).select(M.title, R.rating)
MR.groupBy(M.title).count().sort(desc('count')).take(20)
print 'El tiempo de ejecución es %s segundos' % (str(time.time()-t1))

# Comente sobre el tiempo de ejecución de la consulta, y los resultados de aquél comparados con sus resultados en a
# El tiempo de ejecución es 31.0088620186 segundos
# El tiempo de ejecucuon es mucho mayor al primero pero los resultados son los mismos se puede comprobar comparando los dos RDDs

# Cree una tabla temporal en que los géneros de las películas estén individualizados en una sola columna.
MM = M.flatMap(lambda row: [[row[1], row[2], g] for g in row[0].split('|')])
tableGenre = MM.map(lambda l: Row(movie_id = l[0], title=l[1], genre=l[2]))
schemaGenre = sqlContext.createDataFrame(tableGenre)
schemaGenre.registerTempTable("temporaryGenderTable")

dataComedy = sqlContext.sql("SELECT * from   where genre='Comedy'")


# Encuentre los 5 géneros de películas que más número de ratings poseen
bestGander = sqlContext.sql("SELECT count(movie_id) as rating, genre from temporaryGenderTable GROUP BY genre ORDER BY rating DESC")
bestGander.take(5)

# 5 que tienen mejor promedio de rating.
bestAvgGander = sqlContext.sql("SELECT AVG(movie_id) as rating, genre from temporaryGenderTable GROUP BY genre ORDER BY rating DESC")
bestAvgGander.take(5)

# Comente sobre el hecho de generar una tabla temporal usando como proceso intermedio funciones de Spark.
# Basado en la experiencia en la práctica es relativamente fácil resolver las consultas ya que el lenguaje (SQL)
# de forma natural te facilita mucho el trabajo, hay que destacar que la tabla temporal
# permite que el cálculo de la operación se haga mucho más rápido.


#  Usando la misma tabla temporal anterior (temporaryGenderTable), encuentre la lista de géneros distintos de las películas. (Hint:use “SELECT DISTINCT”).
t1 = time.time()
distinct = sqlContext.sql("SELECT DISTINCT genre  from temporaryGenderTable")
distinct.collect()
print 'El tiempo de ejecución es %s segundos' % (str(time.time()-t1))
# El tiempo de ejecución es 0.64781498909 segundos

# Repita el proceso anterior usando las funciones comunes de Spark,
t1 = time.time()
M.flatMap(lambda x: x[0].split('|')).distinct().collect()
print 'El tiempo de ejecución es %s segundos' % (str(time.time()-t1))
# El tiempo de ejecución es 0.223791122437 segundos
#


