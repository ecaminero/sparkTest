from pyspark import SparkContext
sc = SparkContext()
sc.setCheckpointDir('/user/cloudera')

# abro los datos, los separo en header y data
fid = open('groceries.csv')
data = [f.replace('\n','').replace('\r','').split(",") for f in fid.readlines()]

# creo un RDD de Spark para manejar los datos
rdd_data = sc.parallelize(data)

# importo el modelo de FP-Growth
from pyspark.mllib.fpm import FPGrowth

model = FPGrowth.train(rdd_data, minSupport=0.01, numPartitions=10)
result = model.freqItemsets().collect()

min_num_items = 2

sorted_filtered_result = sorted([(fi[0], fi[1]) for fi in result if len(fi[0])>=min_num_items],\
				 key = lambda x:x[1], reverse=True)

fid = open('output_association.txt','w')
for fi in sorted_filtered_result:
	fid.write(str(fi) + '\n')
fid.close()
