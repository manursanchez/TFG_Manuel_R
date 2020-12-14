#!/usr/bin/env python
from mrjob.job import MRJob

class counter(MRJob):
    def mapper_init(self):
        #Creamos un diccionario con los que serán los contadores a 0
        self.contadores={"Netherlands":0,"France":0,"Australia":0}    
        
    def mapper(self, _, line):
        linea=line.split(";")
        # Estudiamos cada token de la línea recogido
        for token in linea:
            # Si el token está en el diccionario
            if token in self.contadores:
                # Contamos con el contador definido en diccionario
                self.contadores[token]=self.contadores[token]+1
          
    def mapper_final(self):
        yield "Bloque: ",self.contadores
    
if __name__ == '__main__':
    counter.run()
