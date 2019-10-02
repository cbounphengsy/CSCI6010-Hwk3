import MapReduce
import sys

mr = MapReduce.MapReduce()

unique = []

def mapper(record):
    seq = record[0]
    nucleo = record[1]
    nucleo = nucleo[:-10]
    mr.emit_intermediate(seq, nucleo)

def reducer(key, list_of_values):
    for v in list_of_values:
        if v not in unique:
            unique.append(v)
            mr.emit(v)

if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
