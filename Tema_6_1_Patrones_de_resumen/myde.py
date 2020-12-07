#!/usr/bin/env python

from mrjob.job import MRJob
import statistics as st
import re

class myde(MRJob):
    
    def mapper(self, _, line):
       
        linea=line.split(";")
        encontrado=re.search('^[0-9]',linea[0])
        if encontrado!=None:
            valor=linea[5] 
            clave=linea[7] 
            yield clave,float(valor)
        
    def reducer(self, key, values):
        valores=[]
        #sumaValores=0
        #contador=0
       
        for value in values:
            valores.append(value)
            #sumaValores+=value
            #contador=contador+1
            
        #valores.sort()
      
        #Cálculo de la mediana
        """if contador%2==0:
            #Para el número de Claves(key) pares
            indice_1=int(contador//2)
            indice_2=int(indice_1-1)

            valor_1=valores[indice_1]
            valor_2=valores[indice_2]
            mediana=(valor_1+valor_2)/2
        
        else:
            #Para el número de claves(key) impares
            indice=(contador-1)//2
            mediana=valores[indice]"""
            
        #Cálculo de la desviación estándar
        desviacionEstandar=st.pstdev(valores)
        medianaA=st.median(valores)
        yield key, (medianaA,desviacionEstandar)
        
if __name__ == '__main__':
    myde.run()

