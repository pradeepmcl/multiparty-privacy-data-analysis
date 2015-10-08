package edu.ncsu.mas.platys.multiparty_privacy.ml;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

public class ScenarioFilter {

  // Scenario filter types
  public static final String CONSISTENT_RESPONSES_CASE1 = "consistent_responses_case1";
  
  public static final String CONSISTENT_RESPONSES_CASE2 = "consistent_responses_case2";
  
  public static final String CONSISTENT_RESPONSES_CASE3 = "consistent_responses_case3";

  public static Set<String> filterScenarioIds(Connection mConn, String filterType)
      throws SQLException {
    Set<String> scenarioIds = new HashSet<String>();

    if (filterType == null || filterType.equals("null")) {
      try (Statement stmt = mConn.createStatement();
          ResultSet rs = stmt
              .executeQuery("SELECT scenario_id FROM turker_picturesurvey_response");) {
        while (rs.next()) {
          scenarioIds.add(rs.getString("scenario_id"));
        }
      }
    } else if (filterType.equals(CONSISTENT_RESPONSES_CASE1)) {
      try (Statement stmt = mConn.createStatement();
          ResultSet rs = stmt.executeQuery("SELECT scenario_id FROM turker_picturesurvey_response"
              + " GROUP BY scenario_id HAVING group_concat(case1_policy) in ('a,a', 'a,a,a', "
              + "  'b,b', 'b,b,b', 'c,c', 'c,c,c', 'other,other', 'other,other,other')");) {
        while (rs.next()) {
          scenarioIds.add(rs.getString("scenario_id"));
        }
      }
    } else if (filterType.equals(CONSISTENT_RESPONSES_CASE2)) {
      try (Statement stmt = mConn.createStatement();
          ResultSet rs = stmt.executeQuery("SELECT scenario_id FROM turker_picturesurvey_response"
              + " GROUP BY scenario_id HAVING group_concat(case2_policy) in ('a,a', 'a,a,a', "
              + "  'b,b', 'b,b,b', 'c,c', 'c,c,c', 'other,other', 'other,other,other')");) {
        while (rs.next()) {
          scenarioIds.add(rs.getString("scenario_id"));
        }
      }
    } else if (filterType.equals(CONSISTENT_RESPONSES_CASE3)) {
      try (Statement stmt = mConn.createStatement();
          ResultSet rs = stmt.executeQuery("SELECT scenario_id FROM turker_picturesurvey_response"
              + " GROUP BY scenario_id HAVING group_concat(case3_policy) in ('a,a', 'a,a,a', "
              + "  'b,b', 'b,b,b', 'c,c', 'c,c,c', 'other,other', 'other,other,other')");) {
        while (rs.next()) {
          scenarioIds.add(rs.getString("scenario_id"));
        }
      }
    }

    return scenarioIds;
  }
}
