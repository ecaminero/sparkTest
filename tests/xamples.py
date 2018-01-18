#!/usr/bin/python
# -*- coding: utf-8 -*-

txtRDD = sc.textFile('/user/cloudera/spark/emol_economia.txt')
words = txtRDD.flatMap(lambda line: line.split())
txt = words.map(lambda w: (w, 1))

result = txtRDD.take(2)


txtRDD.take(1)


def saludo(nombre):
  return 'hola  %s' % nombre
s = saludo('veronica')  # ---> 'hola veronica'

####
l = ['00001 s1:s2:s3', '00002 s1:s2:s3', '00003 s1:s2:s3']

########################
(lambda nombre: 'hola  %s' % nombre)('vero')

#######
s = lambda nombre: 'hola  %s' % nombre
s('hola')