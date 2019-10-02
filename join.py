import MapReduce
import sys

mr = MapReduce.MapReduce()

def mapper(record):
    orderID = record[1]
    mr.emit_intermediate(orderID, record)

def reducer(key, list_of_values):
    order = []
    lineArray = []
    for v in list_of_values:
        if v[0] == "order":
            order = v
        elif v[0] == "line_item":
            lineArray.append(v)
    for line in lineArray:
        mr.emit(order + line)

if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
