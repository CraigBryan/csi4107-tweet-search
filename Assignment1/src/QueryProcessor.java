import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/*
 * This is the powerhouse class of this assignment. It builds the index,
 * parses and scores the queries, and searching for results and saves the
 * results to a file the trec_eval file can parse.
 */
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

	// Lucene constructs used throughout the three steps
	private Directory index;
	private Directory hashtagIndex;
	private Analyzer analyzer;
	private HashMap<String, Query> queries;

	// Analyzer to use
	public static enum AnalyzerChoice {
		STANDARD, ENGLISH
	}
	
	// Whether or not to use the relevance feedback system
	private boolean useRelevanceFeedback;
	
	// Whether or not to use hashtag scoring
	private boolean useHashtagScoring;

	//Number of relevant documents uesed for feedback
	private final int RELEVANT_DOCUMENTS_CONSIDERED = 10;

	// The coefficients used in the relevance feedback system
	private double originalQueryCoefficient = 1.0;
	private double relevantQueryCoefficient = 5.5;
	private double irrelevantQueryCoefficient = 0;
	
	// The coefficient used for hashtag-based scoring
	private float hashtagScoreCoefficient = 0.1f;
	
	// Indexed, tokenized, stored, Term-Vectors
	public static final FieldType TYPE_STORED = new FieldType();
	
	/* Term-Vectors */
	public static final FieldType TYPE_HASHTAG = new FieldType();

    static {
		 TYPE_STORED.setIndexed(true);
		 TYPE_STORED.setTokenized(true);
		 TYPE_STORED.setStored(true);
		 TYPE_STORED.setStoreTermVectors(true);
		 TYPE_STORED.setStoreTermVectorPositions(true);
		 TYPE_STORED.freeze();
    }
    
    static {
		TYPE_HASHTAG.setIndexed(true);
		TYPE_HASHTAG.setTokenized(true);
		TYPE_HASHTAG.setStored(true);
		TYPE_HASHTAG.setStoreTermVectors(true);
		TYPE_HASHTAG.setStoreTermVectorPositions(true);
		TYPE_HASHTAG.freeze(); 
		
	}
	
	/* Constructor that takes many arguments from the runner.
	 * The first four are file names for input and output
	 * The last two deal with the relevance feedback system
	 */
	public QueryProcessor(String inputTweetsFile, 
						  String inputQueriesFile,
						  String vocabFile,
						  String resultsFile,
						  boolean useRelevanceFeedback,
						  Double[] relevanceCoefficients,
						  boolean useHashtagScoring,
						  Float hashtagScoreCoefficient,
						  AnalyzerChoice ac) {
		this.inputTweetsFile = inputTweetsFile;
		this.inputQueriesFile = inputQueriesFile;
		this.vocabFile = vocabFile;
		this.resultsFile = resultsFile;

		if (ac == AnalyzerChoice.ENGLISH) {
			analyzer = new EnglishAnalyzer(Version.LUCENE_40);
		} else {
			analyzer = new StandardAnalyzer(Version.LUCENE_40);
		}

		//Set any relevance feedback parameters sent from the command line
		this.useRelevanceFeedback = useRelevanceFeedback;
		if(relevanceCoefficients[0] != null) {
			originalQueryCoefficient = relevanceCoefficients[0];
		}

		if(relevanceCoefficients[1] != null) {
			relevantQueryCoefficient = relevanceCoefficients[1];
		}

		if(relevanceCoefficients[2] != null) {
			irrelevantQueryCoefficient = relevanceCoefficients[2];
		}
		
		//Sets the hashtag scoring parameters sent from the command line
		this.useHashtagScoring = useHashtagScoring;
		if(hashtagScoreCoefficient != null) {
			this.hashtagScoreCoefficient = hashtagScoreCoefficient;
		}
	}

	// Main method that calls methods in the correct order
	public void go() {
		index = buildIndex();
		
		if(useHashtagScoring){
			hashtagIndex = buildHashtagIndex();
		}
		analyzeIndex();
		queries = processQueries();
		getResults();
	}

	/* Processes the input documents and builds a hashtag-based index
	 */
	private Directory buildHashtagIndex() {
		
		Directory newHashtagIndex = new RAMDirectory();
	
		 //a wrapper replaced analyzer in the following
		IndexWriterConfig indexConfig = 
			new IndexWriterConfig(Version.LUCENE_40, analyzer);
		
		IndexWriter w = null;
				
		// initialize index
		try {
			w = new IndexWriter(newHashtagIndex, indexConfig);
			
			// add the tweets to the Lucene index 
			parseHashtags(w, inputTweetsFile);

		} catch (IOException e) {
			System.out.println("Error building index");
			e.printStackTrace();
		} finally {
			try { w.close(); } 
			catch (IOException | NullPointerException e) { }
		}

		return newHashtagIndex;
	}

	/* Processes the input documents and builds the index
	 */
	private Directory buildIndex() {
		Directory tweetIndex = new RAMDirectory();

		IndexWriterConfig indexConfig = 
			new IndexWriterConfig(Version.LUCENE_40, analyzer);
		
		IndexWriter w = null;
		
		// initialize index
		try {
			w = new IndexWriter(tweetIndex, indexConfig);

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

	/* Processes the queries from XML and places them in a map with their
	 * ID's.
	 */
	private HashMap<String, Query> processQueries() {
		
		ArrayList<QueryXml> rawQueries = null;
		//QueryParser parser = new QueryParser(Version.LUCENE_40, "tweet" , analyzer);
		
		MultiFieldQueryParser parser = new MultiFieldQueryParser(
				Version.LUCENE_4_8_0, new String[] {"tweet", "Hashtags"}, analyzer);
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

	/**
	 * Scores using TF-IDF, relevance feedback, and minimizes over 
	 * Hashtag indexing + TF-IDF indexing. Then it finds the relevant
	 * documents for each query.
	 */
	private void getResults() {
		IndexReader reader = null;	
		IndexReader hashtagReader = null;
		try {
			reader = DirectoryReader.open(index);
			if(useHashtagScoring){
			hashtagReader = DirectoryReader.open(hashtagIndex);
			}
		} catch (IOException e) {
			System.out.println("Error getting results");
			e.printStackTrace();
		}
		
		IndexSearcher searcher = new IndexSearcher(reader);
		
		IndexSearcher hashtagSearcher = null;  // so compiler wont complain
		if(useHashtagScoring){
			hashtagSearcher = new IndexSearcher(hashtagReader);
		}
		
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
			
			ScoreDoc[] hashtagHits = null; // so compiler wont complain
			
			if(useHashtagScoring){

				TopScoreDocCollector hashtagCollector = TopScoreDocCollector.create(NUM_HITS, true);
				
				try {					
					hashtagSearcher.search(queries.get(qId), hashtagCollector);
				} catch (IOException e) {
					System.out.println("Error getting results");
					e.printStackTrace();
				} 
				hashtagHits = hashtagCollector.topDocs().scoreDocs;
			}
			
			outputBuilder.resetRank();
			
			
		
			//Re-scores hits using relevance feedback
			if(useRelevanceFeedback) {
				hits = evaluateQueryWithRelevanceFeedback(queries.get(qId), searcher, hits, collector);
			}
			
			if(useHashtagScoring) {								
		
				// maps document id to a rank, this will later be used to create a list of IDandScore
				Map<String, Float> scoreMapping = new HashMap<>(1500); // so we have enough size in our table
							
				for(ScoreDoc hit: hits) {
					
					Document d = null;
					
					try {
						d = searcher.doc(hit.doc);
					} catch (IOException e) {
						System.out.println("Error getting results");
						e.printStackTrace();
					}
					
					String id = d.get("id");
					scoreMapping.put(id, hit.score);
				}
			
				for(ScoreDoc hashtagHit: hashtagHits) {
					
					Document d = null;
					
					try {
						d = hashtagSearcher.doc(hashtagHit.doc);
						
					} catch (IOException e) {
						System.out.println("Error getting results");
						e.printStackTrace();
					}
					String id = d.get("id");
					
					if(scoreMapping.containsKey(id)){
						Float hitScore = scoreMapping.get(id);
						scoreMapping.put(id, hitScore + hashtagHit.score);
						
					}
					else{
						scoreMapping.put(id, hashtagHit.score);
					}
				}
				
				List<IDandScore> ranking = new ArrayList<>();
				
				for(String key : scoreMapping.keySet()){									
					ranking.add(new IDandScore(key, scoreMapping.get(key)));					
				}	
				
				Collections.sort(ranking);
			
				int limit = Math.min(ranking.size(), 1000);						
				for(int i = 0; i < limit; i++){
						
					outputBuilder.add(qId, ranking.get(i).id, ranking.get(i).score);			
				
				}
			} else { // if we are not using hashtag scoring
			
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
		}
		
		outputBuilder.close();
	}
	
	/*
	 * Parses the tweet documents from the tweet file
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
				doc.add(new Field("tweet", idAndMessage[1], TYPE_STORED));
				doc.add(new StringField("id", idAndMessage[0], Store.YES));
			/*	
				String hashtags = "";
				
				String[] tweetWords = idAndMessage[1].split(" ");
				
				for(int i = 0; i < tweetWords.length;i++){
					if(tweetWords[i].contains("#")){
						String tweetword = tweetWords[i];
						tweetword = tweetWords[i].replace("#", "");
						hashtags += tweetword + " ";
					}
				}
				//Hashtags could be separated by a white space analyzer (standard instead though)
				Field hashtagField = new Field("Hashtags", hashtags, TYPE_HASHTAG);
				hashtagField.setBoost(hashtagScoreCoefficient);
				doc.add(hashtagField);
			*/
				writer.addDocument(doc);

			} catch(Exception e) {
				System.out.println(tweet);
			}
		}
		
		in.close();
	}
	
	/**
	 * Parses Hashtags from the input file
	 * @param writer
	 * @param fileName
	 * @throws IOException
	 */
	private void parseHashtags(IndexWriter writer, 
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
				
				String hashtags = "";
				
				String[] tweetWords = idAndMessage[1].split(" ");
				
				for(int i = 0; i < tweetWords.length;i++){
					if(tweetWords[i].contains("#")){
						String tweetword = tweetWords[i];
						tweetword = tweetWords[i].replace("#", "");
						hashtags += tweetword + " ";
					}
				}
				//Hashtags could be separated by a white space analyzer (standard instead though)
				Field hashtagField = new Field("Hashtags", hashtags, TYPE_HASHTAG);
				hashtagField.setBoost(hashtagScoreCoefficient);
				doc.add(hashtagField);
				doc.add(new StringField("id", idAndMessage[0], Store.YES));
				writer.addDocument(doc);

			} catch(Exception e) {
				System.out.println(tweet);
			}
		}
		
		in.close();
	}
	
	/*
	 * Gets the queries from the XML in the query file
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

	/* This gets vocabulary data out of the index and saves them to the 
	 * the vocabulary output file.
	 */
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

	private ScoreDoc[] evaluateQueryWithRelevanceFeedback(Query q, 
		IndexSearcher searcher, ScoreDoc[] firstResults, 
		TopScoreDocCollector collector) {
		
		IndexReader reader = searcher.getIndexReader();
		
		QueryParser parser = new QueryParser(Version.LUCENE_40, 
											 "tweet", 
											 analyzer);
		
		// adds the query terms to our existing set of terms 
		// (this helps us manage our vector lengths)
		Set<Term> queryTerms = new HashSet<Term>();
		q.extractTerms(queryTerms);	
		
		Map<String, Double> queryTermMap = new HashMap<String, Double>();
		Map<String, Double> relevantVector = new HashMap<String, Double>();
		Map<String, Double> notRelevantVector = new HashMap<String, Double>();
	
		for(Term t: queryTerms){
			queryTermMap.put(t.toString(), originalQueryCoefficient*(1.00) );
		}
		
		int limit = Math.min(RELEVANT_DOCUMENTS_CONSIDERED, firstResults.length);
		
		for(int i = 0; i < limit; i++) {		
			try {
				ScoreDoc goodHit = firstResults[i];
				ScoreDoc badHit = firstResults[firstResults.length - i - 1];
				
		        relevantVector = getTermFrequencies(reader, goodHit.doc);				
				notRelevantVector = getTermFrequencies(reader, badHit.doc);		
				
				for(String key: relevantVector.keySet()){
					if(!queryTermMap.containsKey(key)){
						queryTermMap.put(key, 0.00);
					}
					queryTermMap.put(key, 
						queryTermMap.get(key) + 
						relevantQueryCoefficient *
					  	(relevantVector.get(key) / 
					  	((double) RELEVANT_DOCUMENTS_CONSIDERED)));
				}
				for(String key: notRelevantVector.keySet()){
					if(!queryTermMap.containsKey(key)){
						queryTermMap.put(key, 0.00);
					}
					queryTermMap.put(key, 
						queryTermMap.get(key) - 
						irrelevantQueryCoefficient *
						(notRelevantVector.get(key) / 
						((double) RELEVANT_DOCUMENTS_CONSIDERED)));
				}
				
				
			} catch (IOException e) {
				System.out.println("Error getting results");
				e.printStackTrace();
			} 
			
		}

		String queryString = "";

		for(String key: queryTermMap.keySet()){
			int frequency = 0;
			frequency = (int) Math.max(0, queryTermMap.get(key));

			for(int i = 0; i < frequency; i++){
				queryString += key + " ";
			}			
		}

		TopScoreDocCollector newCollector = null;

		try {
			Query updatedQuery = parser.parse(queryString);
			newCollector = TopScoreDocCollector.create(NUM_HITS, true);
			searcher.search(updatedQuery, newCollector);
		} catch (IOException e) {
			System.out.println("Error getting results for updated Query");
			e.printStackTrace();
		} catch (ParseException e) {
			System.out.println("Error parsing updated Query");
			e.printStackTrace();
		} 
		if(newCollector.getTotalHits() > 0){
			return newCollector.topDocs().scoreDocs;
		}
		return collector.topDocs().scoreDocs;
	}
	
	/**
	 * Method used for alternative creation of new queries for relevance 
	 * feedback
	 */
	 Directory createIndex(Document d1) throws IOException {

	        Directory directory = new RAMDirectory();
	        SimpleAnalyzer sa = new SimpleAnalyzer(Version.LUCENE_40);
	        IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_40, sa);
	        IndexWriter writer = new IndexWriter(directory, iwc);
	        writer.addDocument(d1);
	        writer.close();
	        return directory;
	    }
	 
	 Map<String, Double> getTermFrequencies(IndexReader reader, int docId)
	            throws IOException {

	        Terms vector = reader.getTermVector(docId, "tweet");
	        TermsEnum termsEnum = null;
	        termsEnum = vector.iterator(termsEnum);
	        Map<String, Double> frequencies = new HashMap<>();
	        BytesRef text = null;

	        while ((text = termsEnum.next()) != null) {
	            String term = text.utf8ToString();
	            int freq = (int) termsEnum.totalTermFreq();
	            frequencies.put(term, (double) freq);
	        }
	        
	        return frequencies;
	    }
	
}
