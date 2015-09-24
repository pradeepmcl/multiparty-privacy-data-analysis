package edu.ncsu.mas.platys.multiparty_privacy.ml;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

public class MTurkIdFilter {

  // MTurk ID filter types
  private static final String HIGH_ARG_RATERS = "high_arg_raters";
  private static final String LOW_ARG_RATERS = "low_arg_raters";
  private static final String CONFLICT_EXPERIENCED = "conflict_experienced";
  private static final String CONFLICT_NOT_EXPERIENCED = "conflict_not_experienced";

  public static Set<String> filterMturkIds(Connection mConn, String filterType)
      throws SQLException {
    Set<String> mturkIds = new HashSet<String>();

    if (filterType == null) {
      try (Statement stmt = mConn.createStatement();
          ResultSet rs = stmt.executeQuery("SELECT mturk_id FROM turker_postsurvey_response");) {
        while (rs.next()) {
          mturkIds.add(rs.getString("mturk_id"));
        }
      }
    } else if (filterType.equals(HIGH_ARG_RATERS)) {
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
              "SELECT mturk_id," + " conflict_experience" + " FROM turker_postsurvey_response");) {
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
              "SELECT mturk_id," + " conflict_experience" + " FROM turker_postsurvey_response");) {
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
}
