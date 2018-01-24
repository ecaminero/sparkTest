
# coding: utf-8

# # Parte I: Uso de funciones comunes de Spark

# In[ ]:


movieText = sc.textFile('/user/cloudera/movies.dat')
ratingText = sc.textFile('/user/cloudera/ratings.dat')


# In[ ]:


ratingsData = ratingText.map(lambda line: line.split(';'))
pairsRating = ratingsData.map(lambda vec: (vec[1], 1))
countRating = pairsRating.reduceByKey(lambda a,b: a+b)


# In[ ]:


movieData = movieText.map(lambda line: line.split(';'))
pairsMovie = movieData.map(lambda vec: (vec[0], vec[1]))


# In[ ]:


sortedData = countRating.sortBy(lambda x: x[1], ascending = False)


# In[ ]:


best = sc.parallelize(sortedData.take(20))
joinedData = best.join(pairsMovie)


# In[ ]:


joinedData.sortBy(lambda x: x[1][0], ascending=False).collect()

