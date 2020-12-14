#!/usr/bin/env python
from mrjob.job import MRJob
import re,os


class unionInterna(MRJob):
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
            if self.namefile=="tiendas.csv":
                linea.append(self.namefile)
                clave=linea[0] 
                yield clave,linea
            else:
                linea.append('art.csv') 
                clave=linea[1]
                yield clave,linea
        
    def reducer(self,key,values):
        listaA=[]
        listaB=[]
        #Llenamos las dos listas
        for valor in values:
            if valor[len(valor)-1]=="tiendas.csv":
                listaA.append(valor)
            else:
                listaB.append(valor)
        
        ########## Union interna ###########
        if listaA and listaB:
            for valor_A in listaA:
                for valor_B in listaB:
                    yield key,(valor_A, valor_B)
                    
if __name__ == '__main__':
    unionInterna.run()
