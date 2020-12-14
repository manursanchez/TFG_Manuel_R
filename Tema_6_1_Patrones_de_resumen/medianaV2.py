#!/usr/bin/env python

from mrjob.job import MRJob
import statistics as st
import re

class medianaV2(MRJob):
    
    def mapper(self, _, line):
       
        linea=line.split(";")
        encontrado=re.search('^[0-9]',linea[5])
        if encontrado!=None:
            valor=linea[5] #UnitPrice
            clave=linea[7] #Country
            yield clave,float(valor)
        
    def reducer(self, key, values):
        valores=list(values)  
        yield key, st.median(valores) #Mediana
        
if __name__ == '__main__':
    medianaV2.run()
