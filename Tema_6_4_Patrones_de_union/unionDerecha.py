#!/usr/bin/env python
from mrjob.job import MRJob
import re,sys,os

class unionDerecha(MRJob):
    
    #Función que limpia lo que el parámetro devuelve en forma "file://nombre_fichero" para dejarlo solo con el nombre 
    #del fichero: "nombre_fichero"
    def limpiarNombreArchivo(self,archivo):
        encontradaBarra=False
        tamano=len(archivo)
        posicion=tamano-1
        while encontradaBarra==False or posicion==0:
            if archivo[posicion]=="/":
                encontradaBarra=True
                return archivo[posicion+1:tamano]
            else:
                posicion-=1
        if posicion==0:
            return archivo
        
    def mapper_init(self):
        self.namefile=self.limpiarNombreArchivo(os.getenv('map_input_file')) 
        
    def mapper(self,_,line):
        clave=""
        linea=line.split(';')
        encontrado=re.search('[a-zA-Z]',linea[0])
        if encontrado==None:
            if self.namefile=="tablaA.csv":
                linea.append(self.namefile)
                clave=linea[0]  
                yield clave,linea
            else:
                linea.append(self.namefile) 
                clave=linea[0] 
                yield clave,linea
    
        
    def reducer(self,key,values):
        listaA=[]
        listaB=[]
        #Llenamos las dos listas
        for valor in values:
            if valor[len(valor)-1]=="tablaA.csv":
                listaA.append(valor)
            else:
                listaB.append(valor)
        
        ##### Union por la derecha #####
        for valorB in listaB:
            if listaA:
                for valorA in listaA:
                    yield valorA, valorB
            else:
                yield "null",valorB
                
if __name__ == '__main__':
    unionDerecha.run()
