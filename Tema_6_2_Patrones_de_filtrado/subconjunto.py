#!/usr/bin/env python
from mrjob.job import MRJob
import re

class subconjunto(MRJob):
    
    def mapper(self,_, line):
        
        #linea=line.rstrip("\n").split(";")
        linea=line.split(";")
        
        #Con una expresión regular comprobamos si lo que entra es una cadena de caracteres o un número
        #Si es un número, nos quedamos con el registro, si no, lo desechamos.
        
        encontrado=re.search('^[0-9]',linea[0]) #La columna 0 tiene que ser un número.
        
        #Aplicamos una condición y lanzamos con yield los que la cumplan
        
        if encontrado!=None and len(linea)>1:
            yield linea[0],linea
            
if __name__ == '__main__':
    subconjunto.run()
