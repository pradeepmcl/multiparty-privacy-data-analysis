package edu.ncsu.mas.platys.multiparty_privacy.ml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.ArffSaver;

public class WekaFeatureBuilder implements AutoCloseable {

  private final Properties mProps = new Properties();

  private Connection mConn;

  public WekaFeatureBuilder()
      throws SQLException, FileNotFoundException, IOException, ClassNotFoundException {
    try (InputStream inStream = new FileInputStream("src/main/resources/application.properties")) {
      mProps.load(inStream);
      Class.forName(mProps.getProperty("jdbc.driverClassName"));
      mConn = DriverManager.getConnection(
          mProps.getProperty("jdbc.url") + "?user=" + mProps.getProperty("jdbc.username")
              + "&password=" + mProps.getProperty("jdbc.password"));
    }
  }

  @Override
  public void close() throws Exception {
    mConn.close();
  }

  private Map<String, Integer[]> readScenarios() throws SQLException {
    Map<String, Integer[]> scenarios = new HashMap<String, Integer[]>();
    try (Statement stmt = mConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT id, image_id, policy_a_id, argument_a_id,"
            + " policy_b_id, argument_b_id, policy_c_id, argument_c_id" + " FROM scenario");) {
      while (rs.next()) {
        Integer[] valuesArray = new Integer[7];
        for (int i = 0; i < 7; i++) {
          valuesArray[i] = rs.getInt(i + 2);
        }
        scenarios.put(rs.getString(1), valuesArray);
      }
    }
    return scenarios;
  }

  private Map<Integer, Integer> getPrefCounts(Map<String, Integer[]> scenarios, String scenarioId) {
    Integer[] scenarioVals = scenarios.get(scenarioId);
    Map<Integer, Integer> countsMap = new HashMap<Integer, Integer>();
    for (int i = 1; i <= 3; i++) {
      countsMap.put(i, 0);
      for (int j = 1; j <= 5; j += 2) {
        if (scenarioVals[j].equals(i)) {
          countsMap.put(i, countsMap.get(i) + 1);
        }
      }
    }
    return countsMap;
  }

  /**
   * 1 is all (a); 2 is common (b); and 3 is self (c)
   * 
   * @param scenarios
   * @param scenarioId
   * @return Greatest of the numbers is most restrictive
   */
  private int getMostRestrictivePolicy(Map<String, Integer[]> scenarios, String scenarioId) {
    Integer[] scenarioVals = scenarios.get(scenarioId);
    int mostRestrictivePolicy = Integer.MIN_VALUE;
    for (int i = 1; i <= 5; i += 2) {
      if (scenarioVals[i] > mostRestrictivePolicy) {
        mostRestrictivePolicy = scenarioVals[i];
      }
    }
    return mostRestrictivePolicy;
  }

  /**
   * 1 is all (a); 2 is common (b); and 3 is self (c)
   * 
   * @param scenarios
   * @param scenarioId
   * @return Smallest of the numbers is least restrictive
   */
  private int getLeastRestrictivePolicy(Map<String, Integer[]> scenarios, String scenarioId) {
    Integer[] scenarioVals = scenarios.get(scenarioId);
    int leastRescrictivePolicy = Integer.MAX_VALUE;
    for (int i = 1; i <= 5; i += 2) {
      if (scenarioVals[i] < leastRescrictivePolicy) {
        leastRescrictivePolicy = scenarioVals[i];
      }
    }
    return leastRescrictivePolicy;
  }

  private int getMajorityPolicy(Map<Integer, Integer> countsMap) {
    int majorityCount = 0;
    int majorityPolicy = 0;

    for (int policy : countsMap.keySet()) {
      if (countsMap.get(policy) > majorityCount) {
        majorityPolicy = policy;
      }
    }
    return majorityPolicy;
  }

  /**
   * First policy is the owner's policy
   * 
   * @param scenarios
   * @param scenarioId
   * @return First policy is the owner's policy
   */
  private int getOwnerPolicy(Map<String, Integer[]> scenarios, String scenarioId) {
    Integer[] scenarioVals = scenarios.get(scenarioId);
    return scenarioVals[1];
  }
  
  private int getArgCount(Map<String, Integer[]> scenarios, String scenarioId, Integer argId) {
    Integer[] scenarioVals = scenarios.get(scenarioId);
    int count = 0;
    for (int i = 2; i <= 6; i += 2) {
      if ((scenarioVals[i] % 3) == (argId % 3)) {
        count++;
      }
    }
    return count;
  }

  private int getArgTypeForPolicy(Map<String, Integer[]> scenarios, String scenarioId,
      Integer policyId) {
    Integer[] scenarioVals = scenarios.get(scenarioId);
    for (int i = 1; i <= 5; i += 2) {
      if (scenarioVals[i] == policyId) {
        return ((scenarioVals[i + 1] - 1) % 3) + 1;
      }
    }

    throw new IllegalArgumentException(
        "Something wrong. Scenario ID: " + scenarioId + " policy ID: " + policyId);
  }
  
  private int getArgForPolicy(Map<String, Integer[]> scenarios, String scenarioId,
      Integer policyId) {
    Integer[] scenarioVals = scenarios.get(scenarioId);
    for (int i = 1; i <= 5; i += 2) {
      if (scenarioVals[i] == policyId) {
        return scenarioVals[i + 1];
      }
    }

    throw new IllegalArgumentException(
        "Something wrong. Scenario ID: " + scenarioId + " policy ID: " + policyId);
  }
  
  /**
   * First argument is the owner's argument
   * 
   * @param scenarios
   * @param scenarioId
   * @return Owner's argument type
   */
  private int getOwnerArgType(Map<String, Integer[]> scenarios, String scenarioId) {
    Integer[] scenarioVals = scenarios.get(scenarioId);
    return ((scenarioVals[2] - 1) % 3) + 1;
  }
  
  /**
   * First argument is the owner's argument
   * 
   * @param scenarios
   * @param scenarioId
   * @return Owner's argument type
   */
  private int getOwnerArg(Map<String, Integer[]> scenarios, String scenarioId) {
    Integer[] scenarioVals = scenarios.get(scenarioId);
    return scenarioVals[2];
  }
  
  public static void main(String[] args)
      throws FileNotFoundException, ClassNotFoundException, SQLException, IOException, Exception {

    String csvFilename = "/home/pmuruka/Dataset/multiparty_privacy/analysis-092415/case123.csv";
    String arffFilename = "/home/pmuruka/Dataset/multiparty_privacy/analysis-092415/case123.arff";

    FastVector atts = new FastVector();

    FastVector likert5AttVals = new FastVector();
    for (int i = 1; i <= 5; i++) {
      likert5AttVals.addElement(Integer.toString(i));
    }

    FastVector relationshipAttVals = new FastVector();
    relationshipAttVals.addElement("family");
    relationshipAttVals.addElement("friends");
    relationshipAttVals.addElement("colleagues");

    FastVector policyAttVals = new FastVector();
    policyAttVals.addElement("a");
    policyAttVals.addElement("b");
    policyAttVals.addElement("c");
    // policyAttVals.addElement("other");

    FastVector argTypeAttVals = new FastVector();
    for (int i = 1; i <= 3; i++) {
      argTypeAttVals.addElement(Integer.toString(i));
    }
    
    FastVector argAttVals = new FastVector();
    for (int i = 1; i <= 36; i++) {
      argAttVals.addElement(Integer.toString(i));
    }
    
    FastVector countAttVals = new FastVector();
    for (int i = 0; i <= 3; i++) {
      countAttVals.addElement(Integer.toString(i));
    }

    // Case 1
    atts.addElement(new Attribute("sensitivity", likert5AttVals));
    atts.addElement(new Attribute("sentiment", likert5AttVals));
    atts.addElement(new Attribute("relationship", relationshipAttVals));

    atts.addElement(new Attribute("case1Policy", policyAttVals));

    // Case 2
    atts.addElement(new Attribute("pref1Count", countAttVals));
    atts.addElement(new Attribute("pref2Count", countAttVals));
    atts.addElement(new Attribute("pref3Count", countAttVals));

    atts.addElement(new Attribute("mostRestrictivePolicy", policyAttVals));
    atts.addElement(new Attribute("leastRestrictivePolicy", policyAttVals));
    atts.addElement(new Attribute("majorityPolicy", policyAttVals));
    
    atts.addElement(new Attribute("ownerPolicy", policyAttVals));

    atts.addElement(new Attribute("case2Policy", policyAttVals));

    // case 3
    atts.addElement(new Attribute("arg1Count", countAttVals));
    atts.addElement(new Attribute("arg2Count", countAttVals));
    atts.addElement(new Attribute("arg3Count", countAttVals));
    
    atts.addElement(new Attribute("argTypeForMostRestrictivePolicy", argTypeAttVals));
    atts.addElement(new Attribute("argTypeForLeastRestrictivePolicy", argTypeAttVals));
    atts.addElement(new Attribute("argTypeForMajorityPolicy", argTypeAttVals));
    
    atts.addElement(new Attribute("ownerArgType", argTypeAttVals));

    atts.addElement(new Attribute("argForMostRestrictivePolicy", argAttVals));
    atts.addElement(new Attribute("argForLeastRestrictivePolicy", argAttVals));
    atts.addElement(new Attribute("argForMajorityPolicy", argAttVals));

    atts.addElement(new Attribute("ownerArg", argAttVals));
    
    atts.addElement(new Attribute("case3Policy", policyAttVals));

    Instances data = new Instances("Responses", atts, 0);

    try (WekaFeatureBuilder featureBuilder = new WekaFeatureBuilder()) {
      Map<String, Integer[]> scenarios = featureBuilder.readScenarios();

      try (Statement stmt = featureBuilder.mConn.createStatement();
          ResultSet rs = stmt.executeQuery("SELECT mturk_id, scenario_id,"
              + " image_sensitivity, image_sentiment, image_relationship,"
              + " case1_policy, case2_policy, case3_policy"
              + " FROM turker_picturesurvey_response");
          PrintWriter csvWriter = new PrintWriter(csvFilename)) {

        while (rs.next()) {
          String mturkId = rs.getString("mturk_id");
          String scenarioId = rs.getString("scenario_id");

          Set<String> mturkIds = MTurkIdFilter.filterMturkIds(featureBuilder.mConn, null);

          if (mturkIds.contains(mturkId)) {
            String case1Policy = rs.getString("case1_policy");
            String case2Policy = rs.getString("case2_policy");
            String case3Policy = rs.getString("case3_policy");

            if (case1Policy.equals("other") || case2Policy.equals("other")
                || case3Policy.equals("other")) {
              continue; // I am ignoring these to be consistent with Ricard
            }

            double[] vals = new double[data.numAttributes()];
            int valInd = 0;

            // Case 1
            vals[valInd++] = likert5AttVals.indexOf(rs.getString("image_sensitivity"));
            vals[valInd++] = likert5AttVals.indexOf(rs.getString("image_sentiment"));
            vals[valInd++] = relationshipAttVals.indexOf(rs.getString("image_relationship"));

            vals[valInd++] = policyAttVals.indexOf(case1Policy);

            // Case 2
            Map<Integer, Integer> countsMap = featureBuilder.getPrefCounts(scenarios, scenarioId);
            for (int i = 1; i <= 3; i++) {
              vals[valInd++] = countsMap.get(i);
            }

            vals[valInd++] = featureBuilder.getMostRestrictivePolicy(scenarios, scenarioId) - 1;
            vals[valInd++] = featureBuilder.getLeastRestrictivePolicy(scenarios, scenarioId) - 1;
            vals[valInd++] = featureBuilder.getMajorityPolicy(countsMap) - 1;

            vals[valInd++] = featureBuilder.getOwnerPolicy(scenarios, scenarioId) - 1;
            
            vals[valInd++] = policyAttVals.indexOf(case2Policy);
            
            // Case 3
            for (int i = 1; i <= 3; i++) {
              vals[valInd++] = featureBuilder.getArgCount(scenarios, scenarioId, i);
            }
            
            vals[valInd++] = argTypeAttVals
                .indexOf(Integer.toString(featureBuilder.getArgTypeForPolicy(scenarios, scenarioId,
                    featureBuilder.getMostRestrictivePolicy(scenarios, scenarioId))));
            vals[valInd++] = argTypeAttVals
                .indexOf(Integer.toString(featureBuilder.getArgTypeForPolicy(scenarios, scenarioId,
                    featureBuilder.getLeastRestrictivePolicy(scenarios, scenarioId))));
            vals[valInd++] = argTypeAttVals
                .indexOf(Integer.toString(featureBuilder.getArgTypeForPolicy(scenarios, scenarioId,
                    featureBuilder.getMajorityPolicy(countsMap))));
            
            vals[valInd++] = featureBuilder.getOwnerArgType(scenarios, scenarioId) - 1;
            
            vals[valInd++] = argAttVals
                .indexOf(Integer.toString(featureBuilder.getArgForPolicy(scenarios, scenarioId,
                    featureBuilder.getMostRestrictivePolicy(scenarios, scenarioId))));
            vals[valInd++] = argAttVals
                .indexOf(Integer.toString(featureBuilder.getArgForPolicy(scenarios, scenarioId,
                    featureBuilder.getLeastRestrictivePolicy(scenarios, scenarioId))));
            vals[valInd++] = argAttVals
                .indexOf(Integer.toString(featureBuilder.getArgForPolicy(scenarios, scenarioId,
                    featureBuilder.getMajorityPolicy(countsMap))));
            
            vals[valInd++] = featureBuilder.getOwnerArg(scenarios, scenarioId) - 1;
            
            vals[valInd++] = policyAttVals.indexOf(case3Policy);

            // Add all cases
            data.add(new Instance(1.0, vals));

            // Write to CSV file (to use in Matlab)
            for (int i = 0; i < vals.length - 1; i++) {
              csvWriter.print(vals[i] + ",");
            }
            csvWriter.print(vals[vals.length - 1] + "\n");
          }
        }
      }
    }

    // Write to ARRF file (to use in Weka)
    ArffSaver saver = new ArffSaver();
    saver.setInstances(data);
    saver.setFile(new File(arffFilename));
    saver.writeBatch();
  }
}
