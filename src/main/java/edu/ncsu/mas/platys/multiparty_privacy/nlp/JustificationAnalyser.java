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
      nlpProps.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner, parse");
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

  public static void main(String[] args) throws FileNotFoundException, ClassNotFoundException,
      IOException, Exception {
    try (JustificationAnalyser justificationAnalyser = new JustificationAnalyser();
        Statement stmt = justificationAnalyser.mConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT case1_policy_justification"
            + " FROM turker_picturesurvey_response")) {

      int i = 0;
      while (rs.next()) {
        String case1PolicyJustification = rs.getString("case1_policy_justification");
        // String case2_policy_justification =
        // rs.getString("case2_policy_justification");
        // String case3_policy_justification =
        // rs.getString("case3_policy_justification");

        if (case1PolicyJustification.contains("Dummy")) {
          continue;
        }

        justificationAnalyser.getSentiment(case1PolicyJustification);
        if (i++ >= 10) {
          break;
        }
      }

    }
  }

}
