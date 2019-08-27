import json
import sys

def getRecords(numRecords, numValues, updated=False):
    recordDict = {}
    for recordId in range(numRecords):
        recordDict[recordId] = ["RecordField" + str(x) + ("updated" if updated else "") for x in range(numValues)]

    return recordDict

# RUN THIS PROGRAM BY RUNNING THIS IN THE CONSOLE: python generateSyncMockData.pyi 10 12
numRecords = int(sys.argv[1])
numValuesPerRecord = int(sys.argv[2])
originalRecords = json.dumps(getRecords(numRecords, numValuesPerRecord))
updatedRecords = getRecords(numRecords, numValuesPerRecord, updated=True)

def getRecordIDsToFilter(records):
    return [key for key in records if key % 2 == 0]

deleteKeys = getRecordIDsToFilter(updatedRecords)
for key in deleteKeys: del updatedRecords[key]

print("originalRecords", len(originalRecords))
file = open("testSparkSyncDataSetOriginal.json","w")
file.write(originalRecords)
file.close()

print("updatedRecords", len(updatedRecords))
file = open("testSparkSyncDataSetUpdated.json","w")
file.write(json.dumps(updatedRecords))
file.close()

