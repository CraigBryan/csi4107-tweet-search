import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;

import java.util.ArrayList;
import java.util.HashMap;

@SuppressWarnings("deprecation")
public class QueryProcessor {

	// Set parameters
	private final int NUM_HITS = 1000;
	private final int NUM_WORDS_TO_PRINT = 100;

	// Input/Output file names
	private String inputTweetsFile;
	private String inputQueriesFile;
	private String vocabFile;
	private String resultsFile;

	// 
	private Directory index;
	private StandardAnalyzer analyzer;
	private HashMap<String, Query> queries;

	
	public QueryProcessor(String inputTweetsFile, 
						  String inputQueriesFile,
						  String vocabFile,
						  String resultsFile) {
		this.inputTweetsFile = inputTweetsFile;
		this.inputQueriesFile = inputQueriesFile;
		this.vocabFile = vocabFile;
		this.resultsFile = resultsFile;

		analyzer = new StandardAnalyzer(Version.LUCENE_40);
	}

	public void go() {
		index = buildIndex();
		analyzeIndex();
		queries = processQueries();
		getResults();
	}

	private Directory buildIndex() {
		Directory tweetIndex = new RAMDirectory();
		
		IndexWriterConfig config = 
			new IndexWriterConfig(Version.LUCENE_40, analyzer);
		
		IndexWriter w = null;
		
		// initialize index
		try {
			w = new IndexWriter(tweetIndex, config);

			// add the tweets to the Lucene index 
			parseTweets(w, inputTweetsFile);

		} catch (IOException e) {
			System.out.println("Error building index");
			e.printStackTrace();
		} finally {
			try { w.close(); } 
			catch (IOException | NullPointerException e) { }
		}

		return tweetIndex;
	}

	private HashMap<String, Query> processQueries() {
		
		ArrayList<QueryXml> rawQueries = null;
		QueryParser parser = 
				new QueryParser(Version.LUCENE_40, "tweet" , analyzer);
		HashMap<String, Query> queryMap = new HashMap<String, Query>(); 
		
		try {
			rawQueries = retrieveQueriesFromTextFile(inputQueriesFile);
		} catch (IOException e) {
			System.out.println("Error parsing input tweets");
			e.printStackTrace();
		}

		for (QueryXml queryXml : rawQueries) {
			
			try {
				Query q = parser.parse(queryXml.title);
				queryMap.put(queryXml.num, q);
			} catch (ParseException e) {
				System.out.println("Error parsing input tweets");
				e.printStackTrace();
			}
		}
		
		return queryMap;
	}

	private void getResults() {
		IndexReader reader = null;	
		try {
			reader = DirectoryReader.open(index);
		} catch (IOException e) {
			System.out.println("Error getting results");
			e.printStackTrace();
		}
		
		IndexSearcher searcher = new IndexSearcher(reader);
		
		OutputBuilder outputBuilder = new OutputBuilder(resultsFile);
		
		for(String qId : queries.keySet()) {
			TopScoreDocCollector collector = TopScoreDocCollector.create(NUM_HITS, true);
			
			try {
				searcher.search(queries.get(qId), collector);
			} catch (IOException e) {
				System.out.println("Error getting results");
				e.printStackTrace();
			} 
			
			ScoreDoc[] hits = collector.topDocs().scoreDocs;
			outputBuilder.resetRank();

			for(ScoreDoc hit: hits) {
				Document d = null;
				
				try {
					d = searcher.doc(hit.doc);
				} catch (IOException e) {
					System.out.println("Error getting results");
					e.printStackTrace();
				}
				
				String id = d.get("id");
				outputBuilder.add(qId, id, hit.score);
			}
		}
		
		outputBuilder.close();
	}
	
	/**
	 * 
	 * @param writer
	 * @param fileName
	 * @throws IOException
	 */
	private void parseTweets(IndexWriter writer, 
							 String fileName) 
							 throws IOException {

		BufferedReader in = new BufferedReader(
							new FileReader(
							new File(fileName)));
	    String tweet;

	    // continuously parse tweets
		while((tweet = in.readLine()) != null){
			
			// add the tweet to the writer
			Document doc = new Document();
			try {
				String[] idAndMessage = tweet.split("\t");
				doc.add(new TextField("tweet", idAndMessage[1], Store.YES));
				doc.add(new StringField("id", idAndMessage[0], Store.YES));
				writer.addDocument(doc);

			} catch(Exception e) {
				System.out.println(tweet);
			}
		}
		
		in.close();
	}
	
	/**
	 * 
	 * @param fileName
	 * @throws IOException 
	 */
	private ArrayList<QueryXml> 
		retrieveQueriesFromTextFile(String fileName) throws IOException {
		
		ArrayList<QueryXml> queries = new ArrayList<QueryXml>();
		BufferedReader in = new BufferedReader(
							new FileReader(
							new File(fileName)));
	    String line;

	    // continuously parse tweets
	    QueryXml query = new QueryXml();
	    boolean open = false;

		while((line = in.readLine()) != null) {
			if(line.startsWith("<top>") || line.startsWith("</top>")) {

				open = !open;
				
				if(open) {	
					query = new QueryXml();

				} else {
					queries.add(query); // this may be flawed
					query = null; 
				}
			}

			// possible bugs introduced if <num> is a query word
			else if(line.startsWith("<num>")){
				line = line.replace("<num> Number: ", "");
				line = line.replace(" </num>", "");
				query.num = line;
			}

			// possible bugs introduced if <title> is a word in the query
			else if(line.startsWith("<title>")){
				line = line.replace("<title> ", "");
				line = line.replace(" </title>", "");
				query.title = line;
			}
			
		}
		
		in.close();

		return queries;
	}

	private void analyzeIndex() {
		IndexReader reader = null;

		try {
			reader = IndexReader.open(index);
		} catch (IOException e) {
			System.out.println("Error during index analysis");
			e.printStackTrace();
		}

		ArrayList<String> wordsArray = new ArrayList<String>();
		
		try {
			Fields fields = MultiFields.getFields(reader);
			Terms terms = fields.terms("tweet");
			TermsEnum iterator =  terms.iterator(null);
			BytesRef byteRef = null;
	
			
			
			while((byteRef = iterator.next()) != null) {
				String term = new String(byteRef.bytes, 
										 byteRef.offset, 
										 byteRef.length);
				wordsArray.add(term);
			}
			
		} catch (IOException e) {
			System.out.println("Error during index analysis");
			e.printStackTrace();
		}

		OutputBuilder writer = new OutputBuilder(vocabFile);

		writer.addRaw("Index Vocabulary Data\n\n");

		int numWords = wordsArray.size();

		writer.addRaw("Number of words: " + numWords + "\n\n");

		writer.addRaw(NUM_WORDS_TO_PRINT 
			+ " random words from the vocabulary:\n");

		int index = 0;
		String word = "";

		for(int i = 0; i < NUM_WORDS_TO_PRINT; i++) {
			index = (int) (Math.random() * (numWords - i));
			word = wordsArray.remove(index);
			
			writer.addRaw(word);
			writer.addRaw("\n");
		}

		writer.close();
	}
}
