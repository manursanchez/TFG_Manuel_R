#!/usr/bin/python

import re
from mrjob.job import MRJob
# Método para cargar el fichero en memoria
def cargarFicheroEnMemoria(self,fichero):
    diccionario={}
    with open(fichero) as f:
        self.tablaEnMemoria = set(line.strip() for line in f)
    for linea in self.tablaEnMemoria:
        #Para que no tenga en cuenta las cabeceras de las tablas
        encontrado=re.search('[a-zA-Z]',linea[0])
        if encontrado==None:
            datos = linea.split(";")
            diccionario[datos[0]]=datos
    return diccionario
class unionReplicadaV2(MRJob):
    
    #archivo para cargarlo en memoria en forma de diccionario
    FILES = ['archivos_datos\\tablaA.csv']
    fichero='tablaA.csv'    
    def mapper_init(self):
        #Nos devuelve la estructura diccionario rellena con los datos del fichero
        self.dicTablaA=cargarFicheroEnMemoria(self,self.fichero)
    
    def mapper(self,_,line):
        self.linea=line.split(';')
        encontrado=re.search('[a-zA-Z]',self.linea[0])#Para que no tenga en cuenta las cabeceras de las tablas
        if encontrado==None:
            #Aplicamos algoritmo: 
            self.clave=self.linea[0] #Se extrae la clave del fichero que llega desde el RUNNER
             # Se comprueba que la clave del fichero de entrada esté en el diccionario
            if self.clave in self.dicTablaA: #Si encuentra la clave en el diccionario unimos y cedemos (yield) resultado
                yield self.clave,(self.linea,self.dicTablaA.get(self.clave) )
            else:#Si no está, sacamos la línea del archivo pero sin su complementario del diccionario de memoria. Uniòn por la izquierda.
                yield self.clave,(self.linea,"null")
            
if __name__ == '__main__':
    unionReplicadaV2.run()
