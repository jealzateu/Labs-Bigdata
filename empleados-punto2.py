from itertools import islice
from statistics import mean
from mrjob.job import MRJob


class Punto2(MRJob):

    def mapper(self, _, line):
        empleadoSalario = []

        for empleados in islice(line.split(','), 0, None, 4):
            for salarios in islice(line.split(','), 2, None, 4):
                if salarios != 'salary' and empleados != 'idemp':
                    empleadoSalario.append([empleados, int(salarios)])

        for x in empleadoSalario:
            yield x[0], x[1]

    def reducer(self, key, values):
        yield key, mean(values)


if __name__ == '__main__':
    Punto2.run()