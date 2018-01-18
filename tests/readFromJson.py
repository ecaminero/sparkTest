#!/usr/bin/python
# -*- coding: utf-8 -*-
# Class 10/2018

from pyspark.sql import Row
import pprint

pp = pprint.PrettyPrinter(indent=4)
path = '/user/cloudera/chatlogs/*.json'
df = sqlContext.read.format('json').load(path) # Read files

def filterAgentByName(agentName):
  # Filter data by James Hicks
  selected = df.filter(
    ((df['agentName'] == agentName) | (df['agentName'] == 'Chris Webb'))
    & (df['accountNum'] < 1000)).select(df['accountNum'], df['conversationId'])

  # show data
  # Opera hasta que llega a los primeros 20 de forma aleatoria (cluster distribuido)
  selected.show() # -Retorna una lista en forma de tabla
  # selected.take(Number) # -Retorna una lista de RDD


def getColumns():
  path = '/user/cloudera/chatlogs/*.json'
  df = sqlContext.read.format('json').load(path) # Read files
  df.columns

def getMessagesByAgentName(agentName):
  messages = df.filter((df['agentName'] == agentName)).select(df['agentName'], df['messages'])
  return messages.take(10)


def joinData(agentName):
  # Filter data by James Hicks
  conversation = df.filter(
    ((df['agentName'] == agentName) | (df['agentName'] == 'Chris Webb'))
    & (df['accountNum'] < 1000)).select(df['accountNum'], df['conversationId'])

  df2 = df.filter(
    (df['category'] == 'Device') &
    (df['accountNum'] < 1000)).select(df['category'], df['agentName'], df['conversationId'])
  dataJoined = conversation.join(df2, on='conversationId', how='inner')
  dataJoined.show()


def mapRddByNameAttribute(attr):
  # agentName - category
  return df.rdd.map(lambda r: r[attr]).take(100)

mapRddByNameAttribute('agentName')


# Using sqlContext on RDD
def usingSQL():
  df.registerTempTable('people')
  df.columns
  data = sqlContext.sql('select * from people' )
  return data.show()
usingSQL()


def getListMessages(row):
   keys = row.asDict().keys()
   keys.remove('messages')
   messages = row['messages']
   elements = []
   elem = {}
   for k in keys:
       elem[k] = row[k]
   for m in messages:
       elem['message'] = m
       elements.append(Row(**elem))
   return elements

listOfMessages = df.flatMap(getListMessages)
##### Notas
# Todos los DataFrame tienen un atributo que es rdd
# Example
# df2.rdd.take(4)
# [Row(category=u'Device', agentName=u'James Hicks', conversationId=u'337652'),
# Row(category=u'Device', agentName=u'Son Paulson', conversationId=u'337735'),
# Row(category=u'Device', agentName=u'Benjamin Brown', conversationId=u'337765'),
# Row(category=u'Device', agentName=u'Son Paulson', conversationId=u'337966')]