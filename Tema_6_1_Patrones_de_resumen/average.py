#!/usr/bin/env python

from mrjob.job import MRJob
import re
class average(MRJob):
    
    def mapper(self,_, line):
        #División del registro para tener la lista de campos
        linea=line.split(";") 
        #El valor del campo [5] tiene que ser un número.
        encontrado=re.search('^[0-9]',linea[5]) 
        
        if encontrado!=None:
        #Extraemos el valor de la lista con el que operaremos
            valor=linea[5] 
        #Extraemos la clave asociada al valor
            clave=linea[6] 
        #Cedemos la clave y el valor al Reducer
            yield clave,float(valor) 
    
    def reducer(self, key, values):
        sumaValues=0 # Para guardar la suma de los valores
        cuentaValues=0 # Para guardar el número de valores 
        # Iteramos a través del conjunto de valores que han
        #llegado desde el mapper
        for value in values:
            sumaValues+=value #Sumamos valor
            cuentaValues+=1 #Contamos valor
        #Dividimos
        mediaValues=sumaValues/cuentaValues
        #Entregamos la media de ventas
        #por país
        yield key, mediaValues

if __name__ == '__main__':
    average.run()
        
