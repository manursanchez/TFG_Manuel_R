#!/usr/bin/env python

from mrjob.job import MRJob
from datetime import datetime
import random
import re

class anonimizar_y_mezclar(MRJob):
    
    def mapper_init(self):
        self.registros = []

    def mapper(self,_, line):
        linea=line.split(";") 
        encontrado=re.search('[a-zA-Z]',linea[3])
        if encontrado==None:
        
            fecha = datetime.strptime(linea[4], '%d/%m/%Y %H:%M') # Truncamos la fecha
            del(linea[6]) # Eliminamos el código del cliente
            linea[4]=fecha.year # Asignamos el año al campo fecha
    
            self.registros.append(linea)
  
    def mapper_final(self):
        for registro in self.registros:
            yield None, registro

    def reducer(self, _, values):
        listaRegistros=[]
        for registro in values:
            listaRegistros.append(registro)
            
        random.shuffle(listaRegistros,random.random) #Los barajo o mezclo
        
        for record in listaRegistros:
            yield _,record

if __name__ == '__main__':
    anonimizar_y_mezclar.run()
