#!/usr/bin/env python
#Segunda versión del patrón de resumen numérico
from mrjob.job import MRJob
import statistics as st
import re
class averageV2(MRJob):
    
    def mapper(self,_, line):
       
        linea=line.split(";")
        encontrado=re.search('^[0-9]',linea[5])
        if encontrado!=None:
            valor=linea[5] #UnitPrice
            clave=linea[7] #Country
            yield clave,float(valor)
        
    def reducer(self, key, values):
        listaValores=[]
        for valor in values:
            listaValores.append(valor)  
        yield key, st.mean(listaValores)
        
        
if __name__ == '__main__':
    averageV2.run()
