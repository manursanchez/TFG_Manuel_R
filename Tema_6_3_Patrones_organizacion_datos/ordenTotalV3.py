#!/usr/bin/env python

from mrjob.job import MRJob
import re

class ordenTotalV3(MRJob):
    
    #Inicializamos la estructura que nos permitirá contener los valores que nos llegan del Map
    def mapper_init(self):
        self.conjuntoRegistros = []
    
    def mapper(self,_, line):
        self.linea=line.rstrip('/n').split(';') 
        
        #Para que no coja la línea de los nombres de campo.
        # si es una cadena de caracteres, no se agrega a la lista.
        encontrado=re.search('[a-zA-Z]',self.linea[1])
                                                
        if encontrado==None:
            #Se agrega el registro completo a la lista
            self.conjuntoRegistros.append(self.linea) 
        
    def mapper_final(self):
        # mando el conjunto de registros al Reducer para reunirlos todos.
        yield self.linea[6], self.conjuntoRegistros
        
        
 #===============================REDUCER=======================================     
        
    def reducer(self, cliente, values):
        listaCompletaRegistros=[]
        
        for valores in values:
            listaCompletaRegistros.extend(valores)
        
        # ordenamos la lista 
        listaCompletaRegistros.sort(key = lambda x: float(x[5])) 
        
        for registro in listaCompletaRegistros:
            yield cliente, registro
        
if __name__ == '__main__':
    ordenTotalV3.run()
