#!/usr/bin/env python

from mrjob.job import MRJob

class hiloeventos(MRJob):
    
    def mapper(self,_, line):
        linea=line.rstrip("\n").split(";")
        if linea[1]=="Alumno5" and linea[3]=="Formulario de entrega visto.":
            yield (linea[1],linea[3]),1
            
if __name__ == '__main__':
    hiloeventos.run()
