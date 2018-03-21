# Modelo de detección de SPAM usando la base de datos CSDMC2010_SPAM

import os
import gensim

from pyspark import SparkContext
sc = SparkContext()

sc.setCheckpointDir('/user/cloudera/checkpoiting')

rejectedChars = ['<>\'\"?+=\r\n']


# Carga de los datos
mailFiles = sc.wholeTextFiles('/user/cloudera/lab-spark-2/TRAINING').map(lambda x: (os.path.basename(x[0]), x[1]
                            .replace('\n','')\
                            .replace('<','')\
                            .replace('>','')\
                            .replace('[','')\
                            .replace(']','')))
labelFiles = sc.textFile('/user/cloudera/lab-spark-2/SPAMTrain.label')


# Separación del set de entrenamiento en conjuntos de entrenamiento y validación

labelList = labelFiles.map(lambda x: x.split()).map(lambda x: (x[1], float(x[0])) )

labelList_train = labelList.filter(lambda x: x[0] < 'TRAIN_03500')
labelList_val = labelList.filter(lambda x: x[0] >= 'TRAIN_03500')


# Creo un RDD con elementos de (label, texto), limpando el texto de caracteres "extraños".

labeledMails = labelList_train.join(mailFiles).map(lambda x: x[1])
cleanLabeledMails = labeledMails.map(lambda x: (x[0], ''.join([w for w in x[1] if w not in rejectedChars])) )


# Transformo los datos "raw" desde texto a listas de palabras lematizadas

cleanLabeledMailsStemmed = cleanLabeledMails.map(lambda x: (x[0], gensim.utils.lemmatize(x[1])) )


# Se crea el vocabulario que después de usará para extraer los descriptores tipo Bag of Words

vocab_raw = cleanLabeledMailsStemmed.flatMap(lambda x: x[1]).map(lambda w: (w,1)).reduceByKey(lambda a,b: a+b)
vocab_filtered = vocab_raw.filter(lambda x: x[1]>10)


# Genero un hashing desde la palabra de vocabulario hacia el índice del vector Bag of Words

vocab = {}
for idx,w in enumerate(vocab_filtered.collect()):
    vocab[w[0]] = idx
num_w = len(vocab)


# Función para generar los datos de entrenamiento en formato LabeledPoint

from pyspark.mllib.feature import LabeledPoint, Vectors

def create_sparse_bow(word_list, vocab, N):
    words = []
    indices = []
    counts = []
    for w in word_list:
        if w in vocab:
            if w in words:
                idx = words.index(w)
                counts[idx] += 1.0
            else:
                words.append(w)
                indices.append(vocab[w])
                counts.append(1.0)
    # sort lists
    idx = [i for i,data in sorted(enumerate(indices), key=lambda x: x[1])]
    return Vectors.sparse(N, [indices[i] for i in idx], [counts[i] for i in idx])


# Creación de los datos de entrenamiento en formato (label, vector de BoW)

trainingData = cleanLabeledMailsStemmed.map( lambda x: LabeledPoint(x[0], create_sparse_bow(x[1], vocab, num_w) ) ).filter(lambda x: x.features.numNonzeros() > 1)


# Entrenamiento del modelo

from pyspark.mllib.classification import NaiveBayes, NaiveBayesModel

model = NaiveBayes.train(trainingData)


# Aplicación del modelo a datos de entrenamiento

predTrain = trainingData.map(lambda x: (x.label, model.predict(x.features))).collect()


# Matriz de confusión en entrenamiento

import matplotlib; matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

def plotConfusionMatrix(conf_arr, alphabet, title='', path='confusion-matrix.png'):
    conf_arr = np.array(conf_arr)
    norm_conf = []
    for i in conf_arr:
        a = 0
        tmp_arr = []
        a = sum(i, 0)
        for j in i:
            tmp_arr.append(float(j)/float(a))
        norm_conf.append(tmp_arr)

    fig = plt.figure()
    plt.clf()
    ax = fig.add_subplot(111)
    ax.set_aspect(1)
    res = ax.imshow(np.array(norm_conf), cmap=plt.cm.jet,
                    interpolation='nearest')

    width, height = conf_arr.shape

    for x in xrange(width):
        for y in xrange(height):
            ax.annotate(str(conf_arr[x][y]), xy=(y, x),
                        horizontalalignment='center',
                        verticalalignment='center')
    plt.title(title)
    plt.xticks(range(width), alphabet[:width])
    plt.yticks(range(height), alphabet[:height])
    plt.savefig(path)


confMatrixTrain = [[0,0],[0,0]]
for r,p in predTrain:
    real = int(r)
    pred = int(p)
    confMatrixTrain[real][pred] += 1

# Guardamos la matriz de confusión en una imagen train-confusion-matrix.png
plotConfusionMatrix(confMatrixTrain, ['SPAM', 'NOT SPAM'], "Matriz de confusion en entrenamiento", "train-confusion-matrix.png")

accuracy_train = sum(1 for p in predTrain if p[0] == p[1])/float(len(predTrain))

print "Accuracy en entrenamiento", accuracy_train


# Aplicación del modelo a datos de validación

labeledMailsVal = labelList_val.join(mailFiles).map(lambda x: x[1])

cleanLabeledMailsVal = labeledMailsVal.map(lambda x: (x[0], ''.join([w for w in x[1] if w not in rejectedChars])) )
cleanLabeledMailsStemmedVal = cleanLabeledMailsVal.map(lambda x: (x[0], gensim.utils.lemmatize(x[1])) )

validationData = cleanLabeledMailsStemmedVal.map( lambda x: LabeledPoint(x[0], create_sparse_bow(x[1], vocab, num_w) ) ).filter(lambda x: x.features.numNonzeros() > 1)

predVal = validationData.map(lambda x: (x.label, model.predict(x.features))).collect()

accuracy_val = sum(1 for p in predVal if p[0] == p[1])/float(len(predVal))

confMatrixVal = [[0,0],[0,0]]
for r,p in predVal:
    real = int(r)
    pred = int(p)
    confMatrixVal[real][pred] += 1

# Guardamos la matriz de confusión en una imagen val-confusion-matrix.png
plotConfusionMatrix(confMatrixVal, ['SPAM', 'NOT SPAM'], "Matriz de confusion en validacion", "val-confusion-matrix.png")

print "Accuracy en validación ", accuracy_val


# Revision de set de test

mailFilesTest = sc.wholeTextFiles('/user/cloudera/lab-spark-2/TESTING')\
                    .map(lambda x: (x[0], x[1]\
                        .replace('\n','')\
                        .replace('<','')\
                        .replace('>','')\
                        .replace('[','')\
                        .replace(']','')) )

cleanMailsTest = mailFilesTest.map(lambda x: (x[0], ''.join([w for w in x[1] if w not in rejectedChars])))

cleanMailsStemmedTest = cleanMailsTest.map(lambda x: (x[0], gensim.utils.lemmatize(x[1])) )

testData = cleanMailsStemmedTest.map( lambda x: (x[0], create_sparse_bow(x[1], vocab, num_w) )) .filter(lambda x: x[1].numNonzeros() > 1)

predTest = testData.map(lambda x: (x[0], model.predict(x[1])) ).map(lambda x: (x[0], 'SPAM' if x[1] == 0.0 else 'NOT SPAM'))

print predTest.sample(False, 0.1).take(20)
