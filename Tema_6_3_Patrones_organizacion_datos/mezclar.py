#!/usr/bin/env python

from mrjob.job import MRJob
import random
import re

class mezclar(MRJob):
    def mapper(self,_, line):
        linea=line.split(";") 
        encontrado=re.search('[a-zA-Z]',linea[3])
        if encontrado==None:
            yield _,linea
  
    def reducer(self, _, values):
        listaRegistros=[]
        for registro in values:
            listaRegistros.append(registro)
        random.shuffle(listaRegistros,random.random)
        for record in listaRegistros:
            yield _,record

if __name__ == '__main__':
    mezclar.run()
