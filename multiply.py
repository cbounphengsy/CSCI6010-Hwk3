import MapReduce
import sys

mr = MapReduce.MapReduce()

finalMatrix = []
count = 0

def mapper(record):
    matrix = record[0]
    i = 0
    if matrix == "a":
        while i < 5:
            #key = cell
            #value = (coressponding column for Matrix B, value of cell of Matrix A)
            mr.emit_intermediate((record[1],i),(record[2],record[3]))
            i += 1
    j = 0
    if matrix == "b":
        while j < 5:
            #key = cell
            #value = (coressponding row for Matrix A, value of cell of Matrix B)
            mr.emit_intermediate((j,record[2]),(record[1],record[3]))
            j += 1

#Times out...
def sortMatrix(matrix):
    i = 0
    x = 0
    y = 0
    while i < 25:
        for cell in matrix:
            if (matrix[i][0] == x) and (matrix[i][1] == y):
                mr.emit(cell)
                if y == 4:
                    x += 1
                    y = 0
                else:
                    y += 1
                i += 1

#To be able to retrieve final matrix with 25 cells
def counter():
    global count
    count += 1

def reducer(key, list_of_values):
    rowA = {}
    colB = {}
    for v in list_of_values:
        if v[0] in rowA:
            #Create Dictionary for column values of Matrix B
            colB[v[0]] = v[1]
        else:
            #Create Dictionary for row values of Matrix A
            rowA[v[0]] = v[1]
    result = 0
    for cell in colB.keys():
        #Matrix Multiplication Matrix A x Matrix B
        result += colB[cell] * rowA[cell]
    #Get matrix of all cells
    finalMatrix.append([key[0],key[1],result])
    counter()
    #Not in row, col order
    #25 is expected number of cells in finalMatrix
    if count == 25:
        for item in finalMatrix:
            mr.emit(item)

    #Tried to order by cell...
    
    #sortMatrix(finalMatrix)
    
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
