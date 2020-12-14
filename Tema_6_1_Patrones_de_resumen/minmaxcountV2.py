#!/usr/bin/env python

from mrjob.job import MRJob
import re

class minmaxcountV2(MRJob):
    
    def mapper(self, _, line):
       
        linea=line.split(";")
        encontrado=re.search('^[0-9]',linea[5]) 
        if encontrado!=None:
            valor=linea[5] 
            clave=linea[6] 
            yield clave,float(valor)
    
    def reducer(self, key, values):
        valores=list(values)
        
        valores.sort()
        vMax=valores[len(valores)-1]
        vMin=valores[0]
        cuenta=len(valores)
        
        yield key, (cuenta,vMax,vMin)
        
if __name__ == '__main__':
    minmaxcountV2.run()
