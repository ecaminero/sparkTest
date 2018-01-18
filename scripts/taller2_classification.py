from pyspark import SparkContext
sc = SparkContext()
sc.setCheckpointDir('/user/cloudera')

# Leo datos de entrenamiento
train_full = sc.textFile('file:///home/cloudera/bank-additional-full.csv')
train_header = train_full.first() #extract header
train_attrib = train_header.replace('"','').replace('\n','').replace('\r','').split(';')
train_data_raw = train_full.filter(lambda row: row != train_header)
train_data = train_data_raw.map(lambda line: line.replace('"','').replace('\n','').replace('\r','').split(';'))
train_data = train_data.filter(lambda vec: len(vec)==21)

#print train_data.take(10)

# Genero datos de atributos y clases
from pyspark.mllib.regression import LabeledPoint
import numpy as np

# Para datos categoricos, se deben convertir a numeros!

cat_data = {}
cat_data[u'job'] 	= train_data.map(lambda x:x[1]).distinct().collect()
cat_data[u'marital'] 	= train_data.map(lambda x:x[2]).distinct().collect()
cat_data[u'education']	= train_data.map(lambda x:x[3]).distinct().collect()
cat_data[u'default']	= train_data.map(lambda x:x[4]).distinct().collect()
cat_data[u'housing']	= train_data.map(lambda x:x[5]).distinct().collect()
cat_data[u'loan']	= train_data.map(lambda x:x[6]).distinct().collect()
cat_data[u'contact']	= train_data.map(lambda x:x[7]).distinct().collect()
cat_data[u'month']	= train_data.map(lambda x:x[8]).distinct().collect()
cat_data[u'day_of_week']	= train_data.map(lambda x:x[9]).distinct().collect()
cat_data[u'poutcome']	= train_data.map(lambda x:x[14]).distinct().collect()
cat_data[u'y']		= train_data.map(lambda x:x[20]).distinct().collect()

print train_attrib

catFeatsInfo = {}
for j in range(len(train_attrib)):
	if train_attrib[j] in cat_data:
		catFeatsInfo[j] = len(cat_data[train_attrib[j]])

def create_labeled_point(vec):
	point = np.zeros(len(vec))
	for i in range(len(train_attrib)):
		if train_attrib[i] in cat_data:
			try:
				point[i] = float(cat_data[train_attrib[i]].index(vec[i]))
			except:
				point[i] = len(cat_data[train_attrib[i]])
		else:
			try:
				point[i] = float(vec[i])
			except:
				point[i] = 0.0
	return LabeledPoint(point[-1], point[:-1])
			
train_data_formatted = train_data.map(create_labeled_point)

# Obtengo el minimo de cada feature para sumarlo
train_min_feat = train_data_formatted.map(lambda x: x.features).reduce(lambda a,b: np.minimum(a,b))

# resto el minimo para tener todos feats positivos (+0)
train_data_formatted_pos = train_data_formatted.map(lambda x: LabeledPoint(x.label, x.features - train_min_feat))

# Clasificacion usando dos modelos
from pyspark.mllib.classification import NaiveBayes
from pyspark.mllib.classification import LogisticRegressionWithSGD

modelNB = NaiveBayes.train(train_data_formatted_pos)
modelLR = LogisticRegressionWithSGD.train(train_data_formatted_pos)

# Leo datos de test
test_full = sc.textFile('file:///home/cloudera/bank-additional.csv')
test_data_raw = test_full.filter(lambda row: row != train_header)
test_data = test_data_raw.map(lambda line: line.replace('"','').replace('\n','').replace('\r','').split(';'))
test_data = test_data.filter(lambda vec: len(vec)==21)

test_data_formatted = test_data.map(create_labeled_point)
test_data_features = test_data_formatted.map(lambda x: x.features - train_min_feat)
test_data_true_label = test_data_formatted.map(lambda x: x.label).collect()

print test_data_true_label[:100]

predictionNB = modelNB.predict(test_data_features).collect()
predictionLR = modelLR.predict(test_data_features).collect()

print predictionNB[:100]
print predictionLR[:100]

# Metricas de rendimiento

# Naive Bayes
TP = float(sum(1 for i in range(len(test_data_true_label)) if test_data_true_label[i] == 0.0 and predictionNB[i] == 0.0))
TN = float(sum(1 for i in range(len(test_data_true_label)) if test_data_true_label[i] == 1.0 and predictionNB[i] == 1.0))
FP = float(sum(1 for i in range(len(test_data_true_label)) if test_data_true_label[i] == 1.0 and predictionNB[i] == 0.0))
FN = float(sum(1 for i in range(len(test_data_true_label)) if test_data_true_label[i] == 0.0 and predictionNB[i] == 1.0))

confMatrixNB = np.array([[TP,FN],[FP, TN]])
accuracyNB = (TP + TN)/(TP+TN+FP+FN)

# Logistic Regression
TP = float(sum(1 for i in range(len(test_data_true_label)) if test_data_true_label[i] == 0.0 and predictionLR[i] == 0.0))
TN = float(sum(1 for i in range(len(test_data_true_label)) if test_data_true_label[i] == 1.0 and predictionLR[i] == 1.0))
FP = float(sum(1 for i in range(len(test_data_true_label)) if test_data_true_label[i] == 1.0 and predictionLR[i] == 0.0))
FN = float(sum(1 for i in range(len(test_data_true_label)) if test_data_true_label[i] == 0.0 and predictionLR[i] == 1.0))

confMatrixLR = np.array([[TP,FN],[FP, TN]])
accuracyLR = (TP + TN)/(TP+TN+FP+FN)

print "Naive Bayes"
print "Confusion Matrix"
print confMatrixNB
print "Accuracy"
print accuracyNB
print "--------------"
print "Logistic Regression"
print "Confusion Matrix"
print confMatrixLR
print "Accuracy"
print accuracyLR








