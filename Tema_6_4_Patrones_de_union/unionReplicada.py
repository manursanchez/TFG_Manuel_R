#!/usr/bin/python
from mrjob.job import MRJob
import re

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

class unionReplicada(MRJob):
   
    FILES = ['hdfs:///archivos_datos/tiendas.csv'] #tiendas-articulos/tiendas.csv']
    fichero='tiendas.csv'    
    def mapper_init(self):
        #Nos devuelve la estructura diccionario rellena con los datos del fichero
        self.dicTablaA=cargarFicheroEnMemoria(self,self.fichero) 
       
    def mapper(self,_,line):
        self.linea=line.split(';')
        encontrado=re.search('[a-zA-Z]',self.linea[0])
        if encontrado==None:
            self.clave=self.linea[1] #Compara Clave de la tabla/s del stream 
            if self.clave in self.dicTablaA: 
                yield self.clave,(self.linea,self.dicTablaA.get(self.clave) )
            else:
                yield self.clave,(self.linea,"null")
                
if __name__ == '__main__':
    unionReplicada.run()
