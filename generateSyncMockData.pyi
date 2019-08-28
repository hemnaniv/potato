import random, sys

def printRecordLine(recordId, numValues):
    recordString = str(recordId)
    for valueId in range(numValues):
        shouldUpdateValue = random.random() > 0.9
        recordString += "," + "recordValue" + str(valueId) + ("updated" if shouldUpdateValue else "")
    return recordString

# def printCSVRecords(numRecords, numValues, updated=False):
#     allRecordCSV = ""
#     for recordId in range(numRecords):
#         shouldDeleteRow = random.random() > 0.7
#         if updated and shouldDeleteRow: continue
#         allRecordCSV += printRecordLine(recordId, numValues) + "\n"
#     return allRecordCSV


def printCSVRecords(numRecords, numValues, file):
    ogCount = 0
    for recordId in range(numRecords):
        shouldDeleteRow = random.random() > 0.7
        if shouldDeleteRow: continue
        line = printRecordLine(recordId, numValues) + "\n"
        ogCount+=1
        file.write(line)
        if ogCount % 10000000 == 0: print("Records created ", ogCount)
    print("Records created ", ogCount)
    print()


# RUN THIS PROGRAM BY RUNNING THIS IN THE CONSOLE: python generateSyncMockData.pyi 10 12
numRecords = int(sys.argv[1])
numValuesPerRecord = int(sys.argv[2])

# originalRecords = printCSVRecords(numRecords, numValuesPerRecord)
# updatedRecords = printCSVRecords(numRecords, numValuesPerRecord, updated=True)




with open("testSparkSyncDataSetOriginal.csv","w") as source_file:
    printCSVRecords(numRecords, numValuesPerRecord, source_file)


with open("testSparkSyncDataSetUpdated.csv","w") as dest_file:
    printCSVRecords(numRecords, numValuesPerRecord, dest_file)
