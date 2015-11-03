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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

public class SentimentAnalyser implements AutoCloseable {

	public static String sentimentString(int sentiment) {
		switch (sentiment) {
		case 0:
			return "Very negative";
		case 1:
			return "Negative";
		case 2:
			return "Neutral";
		case 3:
			return "Positive";
		case 4:
			return "Very positive";
		default:
			return "Unknown sentiment label " + sentiment;
		}
	}

	private final Properties mProps = new Properties();

	private Connection mConn;

	public SentimentAnalyser() throws SQLException, FileNotFoundException,
			IOException, ClassNotFoundException {
		try (InputStream inStream = new FileInputStream(
				"src/main/resources/application.properties")) {
			mProps.load(inStream);
			Class.forName(mProps.getProperty("jdbc.driverClassName"));
			mConn = DriverManager.getConnection(mProps.getProperty("jdbc.url")
					+ "?user=" + mProps.getProperty("jdbc.username")
					+ "&password=" + mProps.getProperty("jdbc.password"));
		}
	}

	public void close() throws Exception {
		mConn.close();
	}

	public static int getSentiment(String text) {
		Properties props = new Properties();
		props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
		StanfordCoreNLP pipeline = new StanfordCoreNLP(props);

		// String text = IOUtils.slurpFileNoExceptions(filename);
		Annotation annotation = new Annotation(text);
		pipeline.annotate(annotation);

		int sentiment = 2;

		int agg_sentiment = 0;
		int sentence_count = 0;

		for (CoreMap sentence : annotation
				.get(CoreAnnotations.SentencesAnnotation.class)) {
			Tree tree = sentence
					.get(SentimentCoreAnnotations.AnnotatedTree.class);
			sentiment = RNNCoreAnnotations.getPredictedClass(tree);
			System.err.println(sentence);
			System.err.println("  Predicted sentiment: "
					+ sentimentString(sentiment));
			agg_sentiment += sentiment;
			sentence_count++;
		}
		if (sentence_count == 0) {
			return 9;
		} else
			return agg_sentiment / sentence_count;
	}

	public static void main(String[] args) throws FileNotFoundException,
			ClassNotFoundException, IOException, Exception {
		// TODO Auto-generated method stub
		String outDir = "data/";
		String csvFilename = outDir + "policy_justification_sentiment.csv";

		try (SentimentAnalyser sentimentAnalyser = new SentimentAnalyser()) {

			try (Statement stmt = sentimentAnalyser.mConn.createStatement();
					ResultSet rs = stmt
							.executeQuery("SELECT mturk_id, "
									+ " case1_policy_justification, case2_policy_justification, case3_policy_justification"
									+ " FROM turker_picturesurvey_response");
					PrintWriter csvWriter = new PrintWriter(csvFilename)) {
				
				int rs_size= 0;
				if (rs != null)   
				{  
				  rs.beforeFirst();  
				  rs.last();  
				  rs_size = rs.getRow();  
				}
				
				System.out.println("Rows: " + rs_size);

				int[] case1_sentiment = new int[rs_size];
				int[] case2_sentiment = new int[rs_size];
				int[] case3_sentiment = new int[rs_size];
				int rs_index = 0;
				
				rs.beforeFirst();
				
				while (rs.next()) {
					String mturkId = rs.getString("mturk_id");
					String case1_policy_justification = rs
							.getString("case1_policy_justification");
					String case2_policy_justification = rs
							.getString("case2_policy_justification");
					String case3_policy_justification = rs
							.getString("case3_policy_justification");

					case1_sentiment[rs_index] = getSentiment(case1_policy_justification);
					case2_sentiment[rs_index] = getSentiment(case2_policy_justification);
					case3_sentiment[rs_index] = getSentiment(case3_policy_justification);

					

					csvWriter.print(mturkId + "," + case1_sentiment[rs_index]
							+ "," + case2_sentiment[rs_index] + ","
							+ case3_sentiment[rs_index] + "\n");
					
					System.out.println(rs_index);
					rs_index++;

				}

			}
		}

	}

}
