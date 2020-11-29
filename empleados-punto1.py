from itertools import islice
from statistics import mean
from mrjob.job import MRJob

class Punto1(MRJob):

    def mapper(self, _, line):
        sectorEconomico = []

        for sectores in islice(line.split(','), 1, None, 3):
            for salarios in islice(line.split(','), 2, None, 4):
                if salarios != 'salary' and sectores != 'economic sector':
                    sectorEconomico.append([sectores, int(salarios)])

        for x in sectorEconomico:
            yield x[0], x[1]

    def reducer(self, key, values):
        yield key, mean(values)


if __name__ == '__main__':
    Punto1.run()