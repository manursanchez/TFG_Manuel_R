#!/usr/bin/env python

from mrjob.job import MRJob
import re
class ordenTotal(MRJob):
    
    #Inicializamos la estructura que nos permitirá contener los valores que nos llegan del Map
    def mapper_init(self):
        self.conjuntoRegistros = []
    
    def mapper(self,_, line):
        linea=line.rstrip('/n').split(';') 
    
        encontrado=re.search('[a-zA-Z]',linea[1])
                                                
        if encontrado==None:
            #Se agrega el registro completo a la lista
            self.conjuntoRegistros.append(linea) 
        
    def mapper_final(self):
        # mando el conjunto de registros al Reducer para reunirlos todos.
        yield None, self.conjuntoRegistros
        
        
 #===============================REDUCER=======================================     
        
    def reducer(self, _, values):
        listaCompletaRegistros=[]
        
        for valores in values:
            listaCompletaRegistros.extend(valores)
        #Expresión Lambda para indicar
        # por qué campo tiene que ordenar la lista.
        listaCompletaRegistros.sort(key = lambda precio: float(precio[5])) 
        
        for registro in listaCompletaRegistros: 
            yield _, registro
        
if __name__ == '__main__':
    ordenTotal.run()
