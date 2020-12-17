#!/usr/bin/env python
from mrjob.job import MRJob

class indexinvertedV2(MRJob):    
    def mapper(self, _, line):
        # Cuidado con el separador
        linea=line.split(' ')
        nLinea=linea[0]
        palabras=linea[1:]
        for palabra in palabras:
            yield palabra.lower(),nLinea
    def reducer(self, key, values):
        for valor in values:
            yield key, valor
    
if __name__ == '__main__':
    indexinvertedV2.run()
