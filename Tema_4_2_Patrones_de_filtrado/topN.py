#!/usr/bin/env python

from mrjob.job import MRJob
import re

TopN=10 #Los N valores más altos que vamos a extraer

class topN(MRJob):
    
    #Método que extrae los N valores más altos de una lista de valores.
    def extraeTopN(self,listaValores):
        valoresMasAltos = []
        cuenta=0
        #Comprobamos que el número de valores Top que necesitamos no sea mayor que el tamaño de la lista
        if TopN>len(listaValores):#si lo es, N lo limitamos al tamaño máximo de la lista.
            nValores=len(listaValores)
        else:
            nValores=TopN
        
        #Buscamos los N valores distintos más altos. Empezamos por el final de la lista
        #que al estar ordenada, los valores más altos están al final. Pero puede haber valores repetidos
        #y eso es lo que vamos a hacer con estas líneas de código, evitar que se repitan valores.
        
        
        posicion=len(listaValores)-1 #Almacenamos la última posición de la lista
        
        while cuenta<nValores: #Como vamos a coger los N valores, "cuenta" la limitamos al número de valores 
                                #que quiera localizar
            try:
                if listaValores[posicion] not in valoresMasAltos: #Si el valor no está en los valores más altos, lo agregamos
                    valoresMasAltos.append(listaValores[posicion])
                    cuenta+=1 # si hemos agregado un valor aumentamos cuenta 
                    
                posicion-=1 # tanto si hemos agregado un valor como si no, disminuimos la posición 
                        # de la lista para buscar un nuevo valor. Puede darse el caso que posición llegue a -1
                        #(cuenta y posición no van al unísono, sobre todo cuando hay valores repetidos)
                        #esto genera una excepción de índice fuera de rango. Es recogida por el try-catch y devuelve
                        #los valores correctos por que ha terminado.
            except:
                break
                
        return valoresMasAltos
    
    #Inicializamos la estructura que nos permitirá contener los valores que nos llegan del Map
    def mapper_init(self):
        self.topNmap = []
    
    def mapper(self,_, line):
        self.registro=line.rstrip('/n').split(';') 
        encontrado=re.search('[a-zA-Z]',self.registro[5])#En este caso el valor lo tengo en la posción 5 de la línea
        #Preguntamos si ha encontrado la expresion regular. Si no la ha encontrado, procesamos dato
        if encontrado==None:
            self.topNmap.append(float(self.registro[5]))
        
    def mapper_final(self):
        self.topNmap.sort() #se ordena la lista
        #La función extraeTopN extrae los 10 elementos últimos de la lista, que se envían a reducer
        yield "TopNmap:", self.extraeTopN(self.topNmap)
        
 #===============================REDUCER=======================================     
        
    def reducer(self, key, values):
        listaValores=[]
        
        # metemos los valores en una lista
        for valores in values:
            listaValores.extend(valores)
        
        # ordenamos la lista    
        listaValores.sort()
        yield "TopNreducer:", self.extraeTopN(listaValores)
        
if __name__ == '__main__':
    topN.run()
