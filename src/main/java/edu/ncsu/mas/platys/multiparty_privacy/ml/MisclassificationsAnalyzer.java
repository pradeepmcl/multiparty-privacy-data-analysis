package edu.ncsu.mas.platys.multiparty_privacy.ml;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class MisclassificationsAnalyzer {

  public static void main(String[] args) throws FileNotFoundException, IOException {
    String misClassificationsFilename = args[0];

    // Add one map for each column in the CSV file
    List<Map<String, Integer>> valueCountsList = new ArrayList<>();
    List<Map<String, Integer>> incorrectValueCountsList = new ArrayList<>();

    try (BufferedReader br = new BufferedReader(new FileReader(misClassificationsFilename))) {
      int lineNum = 0;
      String line;
      while ((line = br.readLine()) != null) {
        String[] values = line.split(",");

        // Only for the first time
        if (lineNum++ == 0) {
          for (int i = 0; i < values.length - 1; i++) {
            valueCountsList.add(new TreeMap<String, Integer>());
            incorrectValueCountsList.add(new TreeMap<String, Integer>());
          }
        }

        for (int i = 0; i < values.length - 1; i++) {
          Map<String, Integer> colCountsMap = valueCountsList.get(i);
          Integer colValCount = colCountsMap.get(values[i]);
          if (colValCount == null) {
            colValCount = 0;
          }
          colCountsMap.put(values[i], ++colValCount);
          
          if (values[values.length - 1].equals("Incorrect")) {
            Map<String, Integer> incorrectColCountsMap = incorrectValueCountsList.get(i);
            Integer incorrectColValCount = incorrectColCountsMap.get(values[i]);
            if (incorrectColValCount == null) {
              incorrectColValCount = 0;
            }
            incorrectColCountsMap.put(values[i], ++incorrectColValCount);
          }

        }
      }
    }
    
    // Print summaries
    for (int i = 0; i < valueCountsList.size(); i++) {
      System.out.println("Column " + (i + 1));
      Map<String, Integer> colCountsMap = valueCountsList.get(i);
      Map<String, Integer> incorrectColCountsMap = incorrectValueCountsList.get(i);
      
      for (String value : colCountsMap.keySet()) {
        Integer count = colCountsMap.get(value);
        Integer incorrectCount = incorrectColCountsMap.get(value);
        if (incorrectCount == null) {
          incorrectCount = 0;
        }
        System.out.print(value + ":" + incorrectCount + "/" + count + "("
            + ((double) incorrectCount / count) + "); ");
      }
      System.out.println();
    }
  }  
}
