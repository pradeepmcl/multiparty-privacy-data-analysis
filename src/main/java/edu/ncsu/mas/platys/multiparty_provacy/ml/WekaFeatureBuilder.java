package edu.ncsu.mas.platys.multiparty_provacy.ml;

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
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.ArffSaver;

public class WekaFeatureBuilder implements AutoCloseable {

  // MTurk ID filter types
  private static final String HIGH_ARG_RATERS = "high_arg_raters";
  private static final String LOW_ARG_RATERS = "low_arg_raters";
  private static final String CONFLICT_EXPERIENCED = "conflict_experienced";
  private static final String CONFLICT_NOT_EXPERIENCED = "conflict_not_experienced";
  
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

  private int getPrefCount(Map<String, Integer[]> scenarios, String scenarioId, Integer prefId) {
    Integer[] scenarioVals = scenarios.get(scenarioId);
    int count = 0;
    for (int i = 1; i <= 5; i += 2) {
      if (scenarioVals[i].equals(prefId)) {
        count++;
      }
    }
    return count;
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
  
  private Set<String> filterMturkIds(String filterType) throws SQLException {
    Set<String> mturkIds = new HashSet<String>();

    if (filterType == null) {
      return mturkIds;
    }
    
    if (filterType.equals(HIGH_ARG_RATERS)) {
      try (Statement stmt = mConn.createStatement();
          ResultSet rs = stmt.executeQuery(
              "SELECT mturk_id," + " preference_confidence, preference_argument_confidence"
                  + " FROM turker_postsurvey_response");) {
        while (rs.next()) {
          int preConf = rs.getInt("preference_confidence");
          int prefArgConf = rs.getInt("preference_argument_confidence");
          if (prefArgConf > preConf) {
            mturkIds.add(rs.getString("mturk_id"));
          }
        }
      }
    } else if (filterType.equals(LOW_ARG_RATERS)) {
      try (Statement stmt = mConn.createStatement();
          ResultSet rs = stmt.executeQuery(
              "SELECT mturk_id," + " preference_confidence, preference_argument_confidence"
                  + " FROM turker_postsurvey_response");) {
        while (rs.next()) {
          int prefConf = rs.getInt("preference_confidence");
          int prefArgConf = rs.getInt("preference_argument_confidence");
          if (prefArgConf <= prefConf) {
            mturkIds.add(rs.getString("mturk_id"));
          }
        }
      }
    } else if (filterType.equals(CONFLICT_EXPERIENCED)) {
      try (Statement stmt = mConn.createStatement();
          ResultSet rs = stmt.executeQuery(
              "SELECT mturk_id," + " conflict_experience"
                  + " FROM turker_postsurvey_response");) {
        while (rs.next()) {
          String conflictExp = rs.getString("conflict_experience");
          if (conflictExp.equals("few") || conflictExp.equals("many")) {
            mturkIds.add(rs.getString("mturk_id"));
          }
        }
      }
    } else if (filterType.equals(CONFLICT_NOT_EXPERIENCED)) {
      try (Statement stmt = mConn.createStatement();
          ResultSet rs = stmt.executeQuery(
              "SELECT mturk_id," + " conflict_experience"
                  + " FROM turker_postsurvey_response");) {
        while (rs.next()) {
          String conflictExp = rs.getString("conflict_experience");
          if (conflictExp.equals("never") || conflictExp.equals("not_sure")) {
            mturkIds.add(rs.getString("mturk_id"));
          }
        }
      }
    }
    return mturkIds;
  }

  public static void main(String[] args)
      throws FileNotFoundException, ClassNotFoundException, SQLException, IOException, Exception {

    String csvFilename = "/home/pmuruka/Dataset/multiparty_privacy/case123_conflictexp.csv";
    String arffFilename = "/home/pmuruka/Dataset/multiparty_privacy/case123_conflictexp.arff";

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
    atts.addElement(new Attribute("case2Policy", policyAttVals));
    
    // case 3
    atts.addElement(new Attribute("arg1Count", countAttVals));
    atts.addElement(new Attribute("arg2Count", countAttVals));
    atts.addElement(new Attribute("arg3Count", countAttVals));
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
          
          Set<String> mturkIds = featureBuilder.filterMturkIds(CONFLICT_EXPERIENCED);
          
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
            for (int i = 1; i <= 3; i++) {
              vals[valInd++] = featureBuilder.getPrefCount(scenarios, scenarioId, i);
            }
            vals[valInd++] = policyAttVals.indexOf(case2Policy);
            
            // Case 3
            for (int i = 1; i <= 3; i++) {
              vals[valInd++] = featureBuilder.getArgCount(scenarios, scenarioId, i);
            }
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
