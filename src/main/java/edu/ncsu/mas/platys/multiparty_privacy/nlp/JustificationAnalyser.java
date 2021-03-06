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
import java.util.HashMap;
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

public class JustificationAnalyser implements AutoCloseable {

  private final Properties mJdbcProps = new Properties();

  private Connection mConn;

  private StanfordCoreNLP mNlpPipeline;

  public JustificationAnalyser() throws SQLException, FileNotFoundException, IOException,
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

  public void getSentiment(String text) {
    Annotation annotation = new Annotation(text);
    mNlpPipeline.annotate(annotation);

    for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
      for (CoreLabel token : sentence.get(TokensAnnotation.class)) {
        String word = token.get(TextAnnotation.class);
        String pos = token.get(PartOfSpeechAnnotation.class);
        String ne = token.get(NamedEntityTagAnnotation.class);
        System.out.println(word + "/" + pos + "/" + ne);
      }

      Tree tree = sentence.get(TreeAnnotation.class);
      System.out.println(tree);
      SemanticGraph dependencies = sentence.get(CollapsedCCProcessedDependenciesAnnotation.class);
      System.out.println(dependencies);
    }
  }

  public Map<String, Integer> getPosDistribution() throws SQLException {
    Map<String, Integer> posCounter = new HashMap<String, Integer>();

    try (Statement stmt = mConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT case1_policy_justification"
            + " FROM turker_picturesurvey_response")) {
      while (rs.next()) {
        String case1PolicyJustification = rs.getString("case1_policy_justification");
        if (case1PolicyJustification.trim().length() < 10) {
          continue;
        }

        Annotation annotation = new Annotation(case1PolicyJustification);
        mNlpPipeline.annotate(annotation);

        for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
          for (CoreLabel token : sentence.get(TokensAnnotation.class)) {
            String pos = token.get(PartOfSpeechAnnotation.class);
            Integer count = posCounter.get(pos);
            if (count == null) {
              posCounter.put(pos, 1);
            } else {
              posCounter.put(pos, count + 1);
            }
          }
        }
      }
    }

    return posCounter;
  }

  public static void main(String[] args) throws SQLException, Exception {
    try (JustificationAnalyser justificationAnalyser = new JustificationAnalyser()) {
      Map<String, Integer> posCounter = justificationAnalyser.getPosDistribution();
      System.out.println(posCounter);
    }
  }
}
