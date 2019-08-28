import random, sys

def printRecordLine(recordId, numValues):
    recordString = str(recordId)
    for valueId in range(numValues):
        shouldUpdateValue = random.random() > 0.9
        recordString += "," + "recordValue" + str(valueId) + ("updated" if shouldUpdateValue else "")
    return recordString

def printCSVRecords(numRecords, numValues, updated=False):
    allRecordCSV = ""
    for recordId in range(numRecords):
        shouldDeleteRow = random.random() > 0.7
        if updated and shouldDeleteRow: continue
        allRecordCSV += printRecordLine(recordId, numValues) + "\n"
    return allRecordCSV


# RUN THIS PROGRAM BY RUNNING THIS IN THE CONSOLE: python generateSyncMockData.pyi 10 12
numRecords = int(sys.argv[1])
numValuesPerRecord = int(sys.argv[2])

originalRecords = printCSVRecords(numRecords, numValuesPerRecord)
updatedRecords = printCSVRecords(numRecords, numValuesPerRecord, updated=True)



print("originalRecords", originalRecords.count("\n"))
file = open("testSparkSyncDataSetOriginal.csv","w")
file.write(originalRecords)
file.close()

print("updatedRecords", updatedRecords.count("\n"))
file = open("testSparkSyncDataSetUpdated.csv","w")
file.write(updatedRecords)
file.close()