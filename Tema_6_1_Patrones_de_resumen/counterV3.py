#!/usr/bin/env python
from mrjob.job import MRJob

contadores={"Netherlands":0,"France":0,"Australia":0} 
lista=[]
class counterV3(MRJob):
    
    def mapper(self, _, line):
        linea=line.split(";")
        # Estudiamos cada token de la línea recogido#
        for token in linea:
            # Si el token está en el diccionario
            if token in contadores:
                # Contamos con el contador definido en diccionario
                contadores[token]=contadores[token]+1
    def mapper_final(self):
        yield "Bloque: ",contadores
            
if __name__ == '__main__':
    counterV3.run()
