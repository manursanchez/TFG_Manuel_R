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
        self.listaA=[]
        self.listaB=[]
    def mapper(self,_,line):
        linea=line.split(';')
        encontrado=re.search('[a-zA-Z]',linea[0])
        if encontrado==None:
            if self.namefile=="tiendas.csv":
                self.listaA.append(linea)
            else:
                self.listaB.append(linea) 
                     
    def mapper_final(self):
        if self.listaA and self.listaB:
            for valor_A in self.listaA:
                for valor_B in self.listaB:
                    yield "valorUnico",(valor_A, valor_B)
        
    """def reducer(self,key,values):
        listaA=[]
        listaB=[]
        #Llenamos las dos listas
        for valor in values:
            if valor[len(valor)-1]=="file_1":
                listaA.append(valor)
            else:
                listaB.append(valor)"""
        
        ########## Producto Cartesiano ###########
        
        
if __name__ == '__main__':
    productoCartesianoV2.run()
