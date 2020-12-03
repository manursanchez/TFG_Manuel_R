#!/usr/bin/env python

from mrjob.job import MRJob
import re,os

class productoCartesianoV2(MRJob):
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
        linea=line.split(';')
        encontrado=re.search('[a-zA-Z]',linea[0])
        if encontrado==None:
            if self.namefile=="tablaA.csv":
                linea.append("file_1")#Añadimos identificador de archivo
                yield "ClaveUnica",linea
            else:
                linea.append("file_2") 
                yield "ClaveUnica",linea
        
    def reducer(self,key,values):
        listaA=[]
        listaB=[]
        #Llenamos las dos listas
        for valor in values:
            if valor[len(valor)-1]=="file_1":
                listaA.append(valor)
            else:
                listaB.append(valor)
        
        ########## Producto Cartesiano ###########
        if listaA and listaB:
            for valor_A in listaA:
                for valor_B in listaB:
                    yield key,(valor_A, valor_B)
        
if __name__ == '__main__':
    productoCartesianoV2.run()
