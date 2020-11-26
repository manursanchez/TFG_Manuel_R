#!/usr/bin/env python
from mrjob.job import MRJob
#Ejemplo sacado de Hadoop with Python
class MRWordCount(MRJob):
    def mapper(self, _, line):
        for word in line.split():
            yield(word, 1)

    def reducer(self, word, counts):
        yield(word, sum(counts))

if __name__ == '__main__':
    MRWordCount.run()
