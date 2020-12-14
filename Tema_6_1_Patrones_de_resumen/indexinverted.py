#!/usr/bin/env python
from mrjob.job import MRJob

class indexinverted(MRJob):
    
    def mapper(self, _, line):
        # Cuidado con el separador
        linea=line.split(' ')
        #Recogemos el primer elemento de la lista "linea", que en este caso identifica el número de línea
        nLinea=linea[0]
        #Recogemos todos los elementos de la lista "linea" a partir de la posición 1 
        palabras=linea[1:]
        #para cada elemento(palabra) de la lista "valor" lo pasamos al Reducer en minúscula y con su número de línea. 
        for palabra in palabras:
            yield palabra.lower(),nLinea
    
    #La característica especial del reducer, es que se va a encargar de hacer un tratamiento por cada palabra que encuentre
    #Lo que vamos a controlar es el número de línea y veces que aparece una palabra en una línea, 
    #mediante una lista. El resto se encarga el Reducer. 
    def reducer(self, key, values):
        listaValores=[]
        listaDef=[]
        #Introducimos en una lista los valores que vienen del mapper
        for valor in values:
            listaValores.append(valor)
        
        #Vamos a ir viendo en cada elemento de la lista que es lo que contiene,
        #para ello usamos el contador "posicion" que nos permitirá recorrer y extraer
        #cada elemento de la lista mediante su número de posición. 
        
        for posicion in range(len(listaValores)):
            # la subLista contendrá el número de línea/documento donde aparece la palabra y 
            # el número de veces que aparece.
            subLista=[]
            
            #Metemos el número de línea/documento en la variable elementoLista
            elementoLista=listaValores[posicion]
            
            #Contamos el número de elementos iguales que hay en listaValores
            #Metemos el número de elementos en la variable numérica cuentaElementos
            cuentaElementos=listaValores.count(elementoLista) 
            
            #Con las dos variables anteriores construimos el elemento de la sublista
            subLista=[listaValores[posicion],cuentaElementos]
            
            #Si esa sublista creada está en la lista definitiva, no la agregamos por que ya ha sido agregada anteriormente
            #si no, lo agregamos a la lista definitiva.
            if subLista not in listaDef:
                listaDef.append(subLista)
                
        #Mandamos a la salida la palabra (key) y las líneas/documentos(listaDef), y número
        #de veces que aparecen
        yield key, listaDef
    
if __name__ == '__main__':
    indexinverted.run()
