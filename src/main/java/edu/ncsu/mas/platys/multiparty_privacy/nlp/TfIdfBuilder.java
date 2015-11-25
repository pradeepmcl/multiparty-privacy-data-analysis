package edu.ncsu.mas.platys.multiparty_privacy.nlp;

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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

public class TfIdfBuilder implements AutoCloseable {

  public enum WordFilterType {
    NONE, POS
  };

  private final Properties mJdbcProps = new Properties();

  private Connection mConn;

  private StanfordCoreNLP mNlpPipeline;

  private Map<Integer, String> idToJustificationMap = new HashMap<Integer, String>();

  // For TF, count terms in each document
  private Table<Integer, String, Integer> docTermCounts = HashBasedTable.create();

  // For IDF, count number of docs for each term
  private Map<String, Integer> termDocCounts = new HashMap<String, Integer>();

  // TF-IDF table
  private Table<Integer, String, Double> tfIdf = HashBasedTable.create();

  public TfIdfBuilder() throws SQLException, FileNotFoundException, IOException,
      ClassNotFoundException {
    try (InputStream inStream = new FileInputStream("src/main/resources/application.properties")) {
      // Load properties file
      mJdbcProps.load(inStream);

      // Open database connection
      Class.forName(mJdbcProps.getProperty("jdbc.driverClassName"));
      mConn = DriverManager.getConnection(mJdbcProps.getProperty("jdbc.url") + "?user="
          + mJdbcProps.getProperty("jdbc.username") + "&password="
          + mJdbcProps.getProperty("jdbc.password"));

      // Open NLP pipleline
      Properties nlpProps = new Properties();
      nlpProps.setProperty("annotators", "tokenize, ssplit, pos");
      mNlpPipeline = new StanfordCoreNLP(nlpProps);
    }
  }

  public void close() throws Exception {
    mConn.close();
    StanfordCoreNLP.clearAnnotatorPool();
  }

  private void readJustifications() throws SQLException {
    try (Statement stmt = mConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT id, case1_policy_justification"
            + " FROM turker_picturesurvey_response")) {
      while (rs.next()) {
        String case1PolicyJustification = rs.getString("case1_policy_justification");

        if (case1PolicyJustification.startsWith("Dummy")
            || case1PolicyJustification.startsWith("dummy")
            || case1PolicyJustification.trim().length() < 10) {
          continue;
        }

        idToJustificationMap.put(rs.getInt("id"), case1PolicyJustification);
      }
    }
  }

  private void updateTermAndDocumentCounts(Integer id, String text, WordFilterType WordFilterType) {
    Set<String> terms = new HashSet<String>();

    Annotation annotation = new Annotation(text);
    mNlpPipeline.annotate(annotation);

    for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
      for (CoreLabel token : sentence.get(TokensAnnotation.class)) {

        String term = token.get(TextAnnotation.class);

        switch (WordFilterType) {
        case NONE:
          break;
        case POS:
          // TODO: Exclude some words, e.g., is, to, because, etc.
          break;
        default:
          continue;
        }

        term = term.toLowerCase();
        
        terms.add(term);

        Integer termCount = docTermCounts.get(id, term);
        if (termCount == null) {
          docTermCounts.put(id, term, 1);
        } else {
          docTermCounts.put(id, term, termCount + 1);
        }
      }
    }

    for (String term : terms) {
      Integer docCount = termDocCounts.get(term);
      if (docCount == null) {
        termDocCounts.put(term, 1);
      } else {
        termDocCounts.put(term, docCount + 1);
      }
    }
  }

  public void buildTfIdf() {
    for (Integer id : idToJustificationMap.keySet()) {
      String justification = idToJustificationMap.get(id);
      updateTermAndDocumentCounts(id, justification, WordFilterType.NONE);
    }

    // Change term counts to term frequencies
    for (Integer docId : docTermCounts.rowKeySet()) {
      Map<String, Integer> termCounts = docTermCounts.row(docId);
      Integer maxCount = Collections.max(termCounts.values());
      for (String term : termCounts.keySet()) {
        double tF = 0.5 + (0.5 * termCounts.get(term) / maxCount);
        double iDF = Math.log((double) idToJustificationMap.size() / termDocCounts.get(term));
        tfIdf.put(docId, term, tF * iDF);
      }
    }
  }

  public static void main(String[] args) throws FileNotFoundException, ClassNotFoundException,
      IOException, Exception {
    try (TfIdfBuilder justificationAnalyser = new TfIdfBuilder()) {
      justificationAnalyser.readJustifications();
      justificationAnalyser.buildTfIdf();

      try (PrintWriter writer = new PrintWriter("term-freqs.txt", "UTF-8")) {
        for (Integer docId : justificationAnalyser.tfIdf.rowKeySet()) {
          Map<String, Double> termFreqs = justificationAnalyser.tfIdf.row(docId);
          for (String term : termFreqs.keySet()) {
            writer.println(docId + "\t" + term + "\t" + termFreqs.get(term));
          }
        }
      }
    }
  }
}
