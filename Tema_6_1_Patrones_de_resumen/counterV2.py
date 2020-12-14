#!/usr/bin/env python
from mrjob.job import MRJob

class counterV2(MRJob):
    
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
        for clave, valor in self.contadores.items(): 
            yield clave,valor
   
    def reducer(self, key, values):
        yield key, sum(values)
        
if __name__ == '__main__':
    counterV2.run()
