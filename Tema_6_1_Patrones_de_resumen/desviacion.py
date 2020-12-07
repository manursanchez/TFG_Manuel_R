#!/usr/bin/env python

from mrjob.job import MRJob
import statistics as st
import re 

class desviacion(MRJob):
    
    def mapper(self, _, line):
       
        linea=line.split(";")
        encontrado=re.search('^[0-9]',linea[0])
        if encontrado!=None:
            valor=linea[5] #UnitPrice
            clave=linea[7] #Country
            yield clave,float(valor)
        
    def reducer(self, key, values):
        valores=[]
        #sumaValores=0
        #contador=0
       
        for value in values:
            valores.append(value)
           
       
        desviacionEstandar=st.pstdev(valores)
        yield key, desviacionEstandar
        
if __name__ == '__main__':
    desviacion.run()

