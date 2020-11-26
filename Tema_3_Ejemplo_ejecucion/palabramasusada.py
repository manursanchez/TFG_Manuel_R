#!/usr/bin/env python
from mrjob.job import MRJob
from mrjob.step import MRStep

import re

WORD_RE = re.compile(r"[\w']+")

class MRMostUsedWord(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_extrae_palabras,
                combiner=self.combiner_cuenta_palabras,
                reducer=self.reducer_cuenta_palabras),
            MRStep(reducer=self.reducer_encuentra_palabra_mas_usada)
        ]
    
    def mapper_extrae_palabras(self, _, line):
        # producimos cada palabra en la linea
        for word in WORD_RE.findall(line):
            yield (word.lower(), 1)
    
    def combiner_cuenta_palabras(self, word, counts):
        # Sumamos las palabras vistas hasta ahora
        yield (word, sum(counts))
    
    def reducer_cuenta_palabras(self, word, counts):
        # Enviamos todo al siguiente reducer
        # para que encuentre la palabra más usada
        yield None, (sum(counts), word)
        
        # Descartamos la clave, nos hace falta la palabra y el número
    def reducer_encuentra_palabra_mas_usada(self, _, word_count_pairs):
        yield max(word_count_pairs)
        
if __name__ == '__main__':
    MRMostUsedWord.run()
