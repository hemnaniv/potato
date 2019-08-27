import json

def getRecords(numRecords, numValues):
    return [{recordId: [("RecordFieldValue%s" % x) for x in range(numValues)]} for recordId in range(0, numRecords)]

x = json.dumps(getRecords(20, 20))
print(x)

file = open("testSparkSyncDataSet.json","w")

file.write(x)
file.close()

