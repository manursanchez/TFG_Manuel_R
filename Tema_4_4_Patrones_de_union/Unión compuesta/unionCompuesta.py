#!/usr/bin/env python

from mrjob.job import MRJob
from mrjob.step import MRStep
import re,os


class unionCompuesta(MRJob):
    
    #Función que limpia lo que el parámetro devuelve en forma "file://nombre_fichero" para dejarlo solo con el nombre 
    #del fichero: "nombre_fichero"
    def limpiarNombreArchivo(self,archivo):
        tamano=len(archivo)
        return archivo[7:tamano]
    # Paso 1 - mapper_init()
    def limpieza_nombre(self):
        self.namefile=self.limpiarNombreArchivo(os.getenv('map_input_file')) #Usamos el parámetro para saber que nos llega desde streaming
        #map_input_file devuelve una cadena de caracteres correspondiente al archivo de entrada por el stream en formato:
        # file://nombre_fichero
       
    # Paso 1 - mapper()
    def entradas(self,_,line):
        linea=line.split(';')
        encontrado=re.search('[a-zA-Z]',linea[0]) #Para que no tenga en cuenta las cabeceras de las tablas
        if encontrado==None:
            if self.namefile=="tablaA.csv":
                yield "listaA",linea
            else:
                yield "listaB",linea
                                          
    # Paso 2 - mapper_init()
    def declara_estructuras(self): 
        self.listaA=[]
        self.listaB=[]
          
    # Paso 2 - Mapper
    def llena_estructuras(self,tabla,line):
       
        if tabla=="listaA":
            self.listaA.append(line)
        else:
            self.listaB.append(line) 
    
    # Paso 2 - mapper_final -->Aquí he de hacer la unión, pero NO FUNCIOONA
    def union_compuesta(self):
        self.listaA.sort(key=lambda x: x[0])
        self.listaB.sort(key=lambda x: x[0])
        """for elemento in self.listaA:
            yield "A: ",elemento
        for elemento in self.listaB:
            yield "B: ",elemento"""
        #if self.listaA and self.listaB:
        for A in self.listaA:
            yield "tabla: ",A
    
    
    def steps(self):
        return [MRStep(mapper_init=self.limpieza_nombre,
                mapper=self.entradas),
               MRStep(mapper_init=self.declara_estructuras,
                     mapper=self.llena_estructuras,
                     mapper_final=self.union_compuesta)
               ]
                        
if __name__ == '__main__':
    unionCompuesta.run()
