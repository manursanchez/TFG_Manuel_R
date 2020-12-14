#!/usr/bin/env python

from mrjob.job import MRJob
import re

class minmaxcount(MRJob):
    
    def mapper(self, _, line):
       
        linea=line.split(";")
        encontrado=re.search('^[0-9]',linea[5]) 
        
        if encontrado!=None:
            valor=linea[5] 
            clave=linea[6] 
            yield clave,float(valor)
        
    def reducer(self, key, values):
        #Lista donde almacenaremos los valores
        valores=[]
        
        #Recorremos la lista de "values"
        #para agregarlos a la lista
        for value in values:
            valores.append(value)
        
        #Ordenamos los valores de la lista
        valores.sort()
        #Valor máximo
        vMax=valores[len(valores)-1] 
        #Valor mínimo
        vMin=valores[0]
        #Número de valores del grupo
        cuenta=len(valores)
        
        yield key, (cuenta,vMax,vMin)
                
if __name__ == '__main__':
    minmaxcount.run()

