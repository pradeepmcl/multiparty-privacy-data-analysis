package edu.ncsu.mas.platys.multiparty_privacy.nlp;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.PartOfSpeechAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations.CollapsedCCProcessedDependenciesAnnotation;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.trees.TreeCoreAnnotations.TreeAnnotation;
import edu.stanford.nlp.util.CoreMap;

public class TfIdfBuilder implements AutoCloseable {

  private final Properties mJdbcProps = new Properties();

  private Connection mConn;

  private StanfordCoreNLP mNlpPipeline;

  private Map<Integer, String> idToJustificationMap = new HashMap<Integer, String>();

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
        
        // case1PolicyJustification.contains("Dummy")
        if (case1PolicyJustification.trim().length() < 10) {
          continue;
        }

        idToJustificationMap.put(rs.getInt("id"), case1PolicyJustification);
      }
    }
  }
  
  public void getPosDistribution() {
    
  }
  
  
  public void getWords(String text) {
    List<String> words = new ArrayList<String>();
    
    Annotation annotation = new Annotation(text);
    mNlpPipeline.annotate(annotation);

    for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
      for (CoreLabel token : sentence.get(TokensAnnotation.class)) {
        String word = token.get(TextAnnotation.class);
        // TODO: Can exclude some words here, e.g., is, to, because, etc.
        words.add(word);
      }
    }
  }

  public static void main(String[] args) throws FileNotFoundException, ClassNotFoundException,
      IOException, Exception {
    try (TfIdfBuilder justificationAnalyser = new TfIdfBuilder()) {
      justificationAnalyser.readJustifications();
    }
  }
}
