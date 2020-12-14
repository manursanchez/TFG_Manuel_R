#!/usr/bin/env python
#Usamos el archivo foros.csv

from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol
import xmlify

class jerarquicoV2(MRJob):
    OUTPUT_PROTOCOL = RawValueProtocol
    def mapper(self,_, line):
        linea=line.split(";") # Cada línea es un mensaje del foro (pregunta, respuesta o comentario)
        
        mensaje=linea[4] # Recogemos el mensaje de la posición 4 de la línea
        tipoMensaje=linea[5] #Recogemos de la posición 5, si es una pregunta, respuesta o comentario
        
        if tipoMensaje=="question": 
            idMensaje=linea[0] #Almacenamos el id único del mensaje
            yield idMensaje,(tipoMensaje,mensaje)
        else:
            idPadre=linea[7] #Almacenamos el identificador del mensaje idMensaje  
            yield idPadre,(tipoMensaje,mensaje)
     
    def reducer(self, key, values):
        diccionario=dict() #Para el caso que usemos diccionarios
        matrizParaXML=[]
        listaPrincipal=[]
        listaAuxiliar=[]
        
        for v in values: #Metemos los valores que vienen en un matriz
            matrizParaXML.append(v) #Matriz que contiene el tipo de mensaje y el mensaje asociado 
        
        for valor in matrizParaXML:
            if valor[0]=="question":#Si es una pregunta la metemos en la lista principal
                listaPrincipal.append(valor[1])
            else:
                listaAuxiliar.append(valor[1]) # Si son respuestas, las vamos agregando a una lista
        
        listaPrincipal.append(listaAuxiliar) #agregamos la lista de respuestas a la lista principal 
        diccionario[key]=listaPrincipal #Para el caso que usemos diccionarios
        yield key,xmlify.dumps(diccionario) # Conversion a XML para el caso que usemos diccionarios
        
if __name__ == '__main__':
    jerarquicoV2.run()
