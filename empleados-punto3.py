from itertools import islice
from mrjob.job import MRJob

class Punto3(MRJob):

    def mapper(self, _, line):
        empleadoSE = []

        for empleados in islice(line.split(','), 0, None, 4):
            for sectores in islice(line.split(','), 1, None, 3):
                if sectores != 'economic sector' and empleados != 'idemp':
                    empleadoSE.append([empleados, sectores])
                    
        for x in empleadoSE:
            yield x[0], x[1]

    def reducer(self, key, values):
        listaSectores = list(values)
        yield key, len(listaSectores)


if __name__ == '__main__':
    Punto3.run()