#!/usr/bin/env python

from mrjob.job import MRJob
import sys, os, re

class ordenTotalV2(MRJob):
    
    #Inicializamos la estructura que nos permitirá contener los valores que nos llegan del Map
    def mapper_init(self):
        self.conjuntoRegistros = []
    
    def mapper(self,_, line):
        linea=line.rstrip('/n').split(';') 
        encontrado=re.search('[a-zA-Z]',linea[1])#Para que no me coja la línea de los nombres de campo. Cojo el primero mismo
                                                # si es una cadena de caracteres, no se agrega a la lista.
        #Preguntamos si ha encontrado la expresion regular (cadena de caracteres). Si no la ha encontrado, entonces
        #agregamos el registro al conjunto de registros que está generando el map.
        if encontrado==None:
            self.conjuntoRegistros.append(linea) #Aquí agrega el registro completo
        
    def mapper_final(self):
        # Si no desactivamos el reducer, se puede ver como salen los conjuntos de cada mapper distribuido en su ejecución
        # paralela en el cluster.
        # mando el conjunto de registros al Reducer para unificarlos en uno solo allí.
        
        #yield "REGISTROS PARA ORDENAR", self.conjuntoRegistros
        
        #Otra versión
        self.conjuntoRegistros.sort(key = lambda x: float(x[5]))
        for registro in self.conjuntoRegistros:
            yield None, registro
        
 #===============================REDUCER=======================================     
        
    def reducer(self, key, values):
        listaCompletaRegistros=[]
        listaCompletaRegistros.extend(values)
       
        listaCompletaRegistros.sort(key = lambda x: float(x[5])) #Usamos la expresión Lambda 
        
        for registro in listaCompletaRegistros:
            yield None, registro
        
if __name__ == '__main__':
    ordenTotalV2.run()
