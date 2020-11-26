#!/usr/bin/env python

from mrjob.job import MRJob

class hiloeventos(MRJob):
    
    def mapper(self,_, line):
        linea=line.rstrip("\n").split(";")
        if linea[1]=="Admin Manuel R." and linea[3]=="Foro: Foro general del curso":
            yield (linea[1],linea[3]),1
            
if __name__ == '__main__':
    hiloeventos.run()
