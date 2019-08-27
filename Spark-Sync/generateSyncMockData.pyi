import json
import sys

def getRecords(numRecords, numValues):
    return [{recordId: [("RecordFieldValue%s" % x) for x in range(numValues)]} for recordId in range(0, numRecords)]


# RUN THIS PROGRAM BY RUNNING THIS IN THE CONSOLE: python generateSyncMockData.pyi 10 12
numRecords = int(sys.argv[1])
numValuesPerRecord = int(sys.argv[2])
x = json.dumps(getRecords(numRecords, numValuesPerRecord))
print(x)

file = open("testSparkSyncDataSet.json","w")
file.write(x)
file.close()

