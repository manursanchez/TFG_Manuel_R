#!/usr/bin/env python

from mrjob.job import MRJob
import re

#Expresion regular para validar ip
ip = ('^(?:(?:25[0-5]|2[0-4][0-9]|'
          '[01]?[0-9][0-9]?)\.){3}'
          '(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$')
IP_RE = re.compile(ip)

class valoresNoRepetidosV2(MRJob):
    
    def mapper(self,_, line):
        linea=line.split(";")
        encontrado=re.search(IP_RE,linea[8])
        if encontrado!=None:    
            yield linea[1],linea[8]
    
    def combiner(self,key,values):
        for valor in values:
            yield key, valor
    def reducer(self, key, values):
        listaDef=[] #Lista para los registros únicos filtrados
        listaControl=[] #Lista para el control de los registros que son repetidos
        
        # metemos los valores en las listas con lo que nos viene del mapper.
        for valor in values:
            #Si el valor de control (en este caso la IP) no está en la lista de control
            #agregamos el valor de control en la lista de control, y el registro completo en la 
            #lista definitiva.
            if valor not in listaControl:
                listaControl.append(valor)
                listaDef.append(valor)
                
        #Podemos sacar por el reducer lo que estimemos        
        for p in listaDef:
            yield key, p
            
if __name__ == '__main__':
    valoresNoRepetidosV2.run()
