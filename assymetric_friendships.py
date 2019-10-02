import MapReduce
import sys

mr = MapReduce.MapReduce()

uniqueArray = []

def mapper(record):
    person = record[0]
    friend = record[1]
    unique = [person, friend]
    uniqueArray.append(unique)
    mr.emit_intermediate(person, friend)

def reducer(key, list_of_values):
    for v in list_of_values:
        unique1 = [key, v]
        unique2 = [v, key]
        if (unique1 in uniqueArray) and (unique2 not in uniqueArray):
            #Not in the same order; no variable to really sort it correctly
            mr.emit(unique1)

if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
