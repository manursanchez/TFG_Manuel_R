#!/usr/bin/env python
#Usamos el archivo foros.csv

from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol
import xmlify

class jerarquico(MRJob):
    
    OUTPUT_PROTOCOL = RawValueProtocol
    
    def mapper(self,_, line):
        linea=line.split(";") 
        
        mensaje=linea[4] # Recogemos el mensaje de la posición 4 de la línea
        tipoMensaje=linea[5] #Recogemos de la posición 5, si es una pregunta o respuesta
       
        if tipoMensaje=="question": 
            idMensaje=linea[0] #Almacenamos el id único del mensaje
            yield idMensaje,(tipoMensaje,mensaje)
        else:
            idMensaje=linea[7] #Almacenamos el identificador del mensaje idMensaje  
            yield idMensaje,(tipoMensaje,mensaje)
     
    def reducer(self, key, values):
        listaValores=[]
        listaPrincipal=[]
        listaAuxiliar=[] 
        
        for v in values: #Metemos los valores que vienen en un matriz
            listaValores.append(v) #Matriz que contiene el tipo de mensaje y el mensaje asociado 
        
        for valor in listaValores:
            if valor[0]=="question":#Si es una pregunta la metemos en la lista principal
                listaPrincipal.append(valor[1])
            else:
                listaAuxiliar.append(valor[1]) # Si son respuestas, las vamos agregando a una lista
        
        listaPrincipal.append(listaAuxiliar) #agregamos la lista de respuestas a la lista principal
        
        #Conversion a XML indicando en el raiz el id del mensaje
        yield "Creada linea XML: " ,xmlify.dumps(listaPrincipal,root = key) 
        
if __name__ == '__main__':
    jerarquico.run()
