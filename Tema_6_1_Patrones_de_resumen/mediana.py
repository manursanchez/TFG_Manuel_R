#!/usr/bin/env python

from mrjob.job import MRJob
import re

class mediana(MRJob):
    
    def mapper(self, _, line):
       
        linea=line.split(";")
        encontrado=re.search('^[0-9]',linea[5])
        if encontrado!=None:
            valor=linea[5] # UnitPrice
            clave=linea[7] # Country
            yield clave,float(valor)
        
    def reducer(self, key, values):
        valores=[]
        sumaValores=0
        contador=0
        #Recorremos los valores
        for value in values:
            valores.append(value)
            sumaValores+=value
            contador=contador+1
        #Ordenamos los valores
        valores.sort()
      
        #Comprobamos si el total de 
        #los valores es par o impar
        
        if contador%2==0:
            #Para el número de Claves(key) pares
            
            #Calculamos los índices
            #donde se encuentran los valores
            #centrales
            indice_1=int(contador//2)
            indice_2=int(indice_1-1)
            
            #Extraemos los valores
            valor_1=valores[indice_1]
            valor_2=valores[indice_2]
            mediana=(valor_1+valor_2)/2
        
        else:
            #Para el número de claves(key) impares
            indice=(contador-1)//2
            mediana=valores[indice]
            
        yield key, mediana
        
if __name__ == '__main__':
    mediana.run()

