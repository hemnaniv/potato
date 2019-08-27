package com.company;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class Main {

    private static Logger log = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) {

        String sourceDataSetFileName = "testSparkSyncDataSetOriginal.json";
        String destinationDatSetFileName = "testSparkSyncDataSetUpdated.json";

        Map<String, List<String>> source = parseRecords(sourceDataSetFileName);
        Map<String, List<String>> destination = parseRecords(destinationDatSetFileName);

        if (source != null && destination != null) {
            Merge merger = new Merge();
            merger.mergeRecords(source, destination);
        }
    }

    /**
     * Parses the data from the file and maps it into the model represented.
     *
     * @param dataSetFileName The file name where the data set is available
     * @return Returns map of record keys and the record values.
     * Returns an empty hashMap in case of error.
     */
    private static Map<String, List<String>> parseRecords(String dataSetFileName) {
        ObjectMapper mapper = new ObjectMapper();

        try {
            byte[] data = Files.readAllBytes(Paths.get(dataSetFileName));
            
            return new ObjectMapper().readValue(data, HashMap.class);
        } catch (IOException e) {
            log.severe("Error reading data from file: " + dataSetFileName + " with exception: " + e.getMessage());
            return null;
        }
    }
}
