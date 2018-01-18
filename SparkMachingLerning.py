
# coding: utf-8

# In[1]:


from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.mllib.classification import LogisticRegressionModel
from pyspark.mllib.regression import LabeledPoint

def parsePoint(line):
    values = [float(x) for x in line.split(' ')]
    return LabeledPoint(values[0], values[1:])


# In[2]:


get_ipython().system(u'head -n 3 /home/cloudera/data_sample.txt')


# In[3]:


data = sc.textFile("file:///home/cloudera/data_sample.txt")
parsedData = data.map(parsePoint)

parsedData.take(3)


# In[4]:


import matplotlib.pyplot as pl
import numpy as np

sampledData = parsedData.sample(withReplacement=False, fraction=0.2)

sampledDataPos = sampledData.filter(lambda x: x.label == 1.0).map(lambda x: x.features).collect()
sampledDataNeg = sampledData.filter(lambda x: x.label == 0.0).map(lambda x: x.features).collect()


# In[5]:


sampledDataPosArray = np.array([x.toArray().tolist() for x in sampledDataPos])
sampledDataNegArray = np.array([x.toArray().tolist() for x in sampledDataNeg])

pl.scatter(sampledDataPosArray[:,0], sampledDataPosArray[:,1])
pl.scatter(sampledDataNegArray[:,0], sampledDataNegArray[:,1])
pl.show()


# In[6]:


model = LogisticRegressionWithLBFGS.train(parsedData)


# In[7]:


labelsAndPreds = parsedData.map(lambda p: (p.label, model.predict(p.features)))
labelsAndPreds.sample(False, 0.03).collect()


# In[8]:


trainErr = labelsAndPreds.filter(lambda (v, p): v != p).count() / float(parsedData.count())
print("Training Error = " + str(trainErr))


# In[10]:


model.save(sc, "model_ex2.mod")
sameModel = LogisticRegressionModel.load(sc, "model_ex2.mod")


# In[11]:


from pyspark.mllib.stat import Statistics
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.linalg.distributed import RowMatrix

feats = parsedData.map(lambda p: p.features)
Mdata = RowMatrix(parsedData.map(lambda p: p.features))


# In[12]:


feats.take(4)


# In[13]:


Mdata.numRows()


# In[14]:


summary = Statistics.colStats(feats)


# In[15]:


print(summary.mean())
print(summary.variance())
print(summary.numNonzeros())

