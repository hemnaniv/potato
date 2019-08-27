package com.company;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Merge {

    public void mergeRecords(Map<String, List<String>> source, Map<String, List<String>> destination) {
        Map<String, List<String>> recordsToUpdate = new HashMap<>();
        List<String> recordsToDelete = new ArrayList<>();

        int deletedRecordsCount = 0;
        int updatedRecordsCount = 0;
        int insertedRecordsCount = 0;


        for (String destRecordKey : destination.keySet()) {
            List<String> destinationRecord = destination.get(destRecordKey);

            List<String> sourceRecord = source.get(destRecordKey);
            if (sourceRecord == null) {
                recordsToDelete.add(destRecordKey);
                deletedRecordsCount++;
            } else {

                if (anyFieldValueHasChanged(sourceRecord, destinationRecord)) {
                    recordsToUpdate.put(destRecordKey, sourceRecord);
                    updatedRecordsCount++;
                    source.remove(destRecordKey);
                }
            }
        }

        Map<String, List<String>> recordsToInsert = source;
        insertedRecordsCount = recordsToInsert.size();
    }

    private boolean anyFieldValueHasChanged(List<String> sourceRecord, List<String> destinationRecord) {
        for (int i = 0; i < sourceRecord.size(); i++) {
            if (!sourceRecord.get(i).equals(destinationRecord.get(i)))
                return true;
        }
        return false;
    }
}
