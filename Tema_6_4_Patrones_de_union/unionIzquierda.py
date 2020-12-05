#!/usr/bin/env python
from mrjob.job import MRJob
import re,sys,os

class unionIzquierda(MRJob):
    
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
                clave=linea[0] #Esta clave es la común de la tabla 1
                yield clave,linea
            else:
                linea.append(self.namefile) 
                clave=linea[1] #Clave común de la tabla 2
                yield clave,linea
       
    ############# Reducer ############################
        
    def reducer(self,key,values):
        listaA=[]
        listaB=[]
        #Llenamos las dos listas
        for valor in values:
            if valor[len(valor)-1]=="tiendas.csv":
                listaA.append(valor)
            else:
                listaB.append(valor)
        
        ###### Union por la izquierda ######
        for valorA in listaA:
            if listaB:
                for valorB in listaB:
                    yield valorA, valorB
            else:
                #Si la listaB está vacía
                yield valorA, "null" 
                
if __name__ == '__main__':
    unionIzquierda.run()
