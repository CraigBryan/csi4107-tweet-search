import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
	
	private Directory hashtagIndex;
	
	
	private StandardAnalyzer analyzer;
	
	// we might not need to use this
	private WhitespaceAnalyzer whiteSpaceAnalyzer;
	
	private HashMap<String, Query> queries;

	private boolean useHashtagScoring = true;
	
	private boolean useRelevanceFeedback = true;
	
	private double originalQueryCoefficient = 1.00;
	private double relevantQueryCoefficient = 5.50;
	private double irrelevantQueryCoefficient = 0.00;
	
	private float hashtagScoreCoefficient = 1.00f;
	
	
	 /* Indexed, tokenized, stored, Term-Vectors */
    	public static final FieldType TYPE_STORED = new FieldType();

    	static {
      	 TYPE_STORED.setIndexed(true);
         TYPE_STORED.setTokenized(true);
         TYPE_STORED.setStored(true);
       	 TYPE_STORED.setStoreTermVectors(true);
       	 TYPE_STORED.setStoreTermVectorPositions(true);
       	 TYPE_STORED.freeze(); 	// not sure what this does
    	}
    	
    	/* Term-Vectors */
    	public static final FieldType TYPE_HASHTAG = new FieldType();

    	static {
    		TYPE_HASHTAG.setIndexed(true);
    		TYPE_HASHTAG.setTokenized(true);
    		TYPE_HASHTAG.setStored(true);
    		TYPE_HASHTAG.setStoreTermVectors(true);
    		TYPE_HASHTAG.setStoreTermVectorPositions(true);
    		TYPE_HASHTAG.freeze(); 	// not sure what this does
    		
    	}
		
	private final int RELEVANT_DOCUMENTS_CONSIDERED = 20;
	public QueryProcessor(String inputTweetsFile, 
						  String inputQueriesFile,
						  String vocabFile,
						  String resultsFile) {
		this.inputTweetsFile = inputTweetsFile;
		this.inputQueriesFile = inputQueriesFile;
		this.vocabFile = vocabFile;
		this.resultsFile = resultsFile;

		analyzer = new StandardAnalyzer(Version.LUCENE_40);
		whiteSpaceAnalyzer = new WhitespaceAnalyzer(Version.LUCENE_4_9_0);
	}

	public void go() {
		index = buildIndex();
		if(useHashtagScoring){
		hashtagIndex = buildHashtagIndex();
		}
		analyzeIndex();
		queries = processQueries();
		getResults();
	}

	private Directory buildHashtagIndex() {
		
		Directory newHashtagIndex = new RAMDirectory();
	
		 
		 //awrapper replaced analyzer in the following
		IndexWriterConfig indexConfig = 
			new IndexWriterConfig(Version.LUCENE_40, analyzer); // possibly use whitespace analyzer
		
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

	private Directory buildIndex() {
		Directory tweetIndex = new RAMDirectory();
		
		/* alternative code
		// sets a different analyzer for the various fields
		 Map<String,Analyzer> analyzerPerField = new HashMap<String,Analyzer>();
		 analyzerPerField.put("HashTags", whiteSpaceAnalyzer);
		 
		 PerFieldAnalyzerWrapper aWrapper =
		   new PerFieldAnalyzerWrapper(analyzer, analyzerPerField);
		 *
		 */
		 //awrapper could replace analyzer in the following
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

	private HashMap<String, Query> processQueries() {
		
		ArrayList<QueryXml> rawQueries = null;
		//QueryParser parser = new QueryParser(Version.LUCENE_40, "tweet" , analyzer);
		
		MultiFieldQueryParser parser = new MultiFieldQueryParser(Version.LUCENE_4_8_0, new String[] {"tweet", "Hashtags"}, analyzer);
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
 * Scores using TF-IDF, relevance feedback, and minimizes over Hashtag indexing + TF-IDF indexing
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
			if(useRelevanceFeedback){
				hits = evaluateQueryWithRelevanceFeedback(queries.get(qId), searcher, hits, collector);
			}
			
			if(useHashtagScoring){								
	
			// maps document id to a rank, this will later be used to create a list of IDandScore
			Map<String, Float> scoreMapping = new HashMap<>(1500); // so we have enough size in our table
						
			// comment out to test hashtags only
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
			ranking.sort(new Comparator<IDandScore>(){
			@Override
			   public int compare(final IDandScore a, final IDandScore b) {
			    return (-1) * a.score.compareTo(b.score);
			     }
			});
		
			
			int limit = Integer.min(ranking.size(), 1000);						
			for(int i = 0; i < limit; i++){
					
				outputBuilder.add(qId, ranking.get(i).id, ranking.get(i).score);			
			
			}
			}
			else{ // if we are not using hashtag scoring
			
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
	
	/**
	 * Parses Tweets from the input file
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

	
	private ScoreDoc[] evaluateQueryWithRelevanceFeedback(Query q, IndexSearcher searcher, ScoreDoc[] firstResults, TopScoreDocCollector collector){
		
		IndexReader reader = searcher.getIndexReader();
		
		QueryParser parser = new QueryParser(Version.LUCENE_40, "tweet" , analyzer);
		
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
		
		//RealVector queryVector = toRealVector(queryTermMap);
		int limit = Integer.min(RELEVANT_DOCUMENTS_CONSIDERED, firstResults.length);
		for(int i = 0; i < limit; i++)/*for(ScoreDoc hit: firstResults)*/ {		
			try {
				
				ScoreDoc goodHit = firstResults[i];
				ScoreDoc badHit = firstResults[firstResults.length - i - 1];
				

		        relevantVector = getTermFrequencies(reader, goodHit.doc);				
				notRelevantVector = getTermFrequencies(reader, badHit.doc);
				
				// Alternatively,the following could be used 
				//Directory directory = createIndex(searcher.doc(goodHit.doc));
				//IndexReader readerForHits = DirectoryReader.open(directory); 
				//relevantVector = getTermFrequencies(readerForHits, 0);
				
				
				for(String key: relevantVector.keySet()){
					if(!queryTermMap.containsKey(key)){
						queryTermMap.put(key, 0.00);
					}
					queryTermMap.put(key, queryTermMap.get(key) + relevantQueryCoefficient*(relevantVector.get(key) / ((double) RELEVANT_DOCUMENTS_CONSIDERED)));
				}
				for(String key: notRelevantVector.keySet()){
					if(!queryTermMap.containsKey(key)){
						queryTermMap.put(key, 0.00);
					}
					queryTermMap.put(key, queryTermMap.get(key) - irrelevantQueryCoefficient*(notRelevantVector.get(key) / ((double) RELEVANT_DOCUMENTS_CONSIDERED)));
				}
				
				
			} catch (IOException e) {
				System.out.println("Error getting results");
				e.printStackTrace();
			} 
			
		}
		String queryString = "";
		for(String key: queryTermMap.keySet()){
			int frequency = 0;
			frequency = (int) Double.max(0, queryTermMap.get(key)); //I think casting as an int will round
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
	 * Method used for alternative creation of new queries for relevance feedback
	 * @param d1
	 * @return
	 * @throws IOException
	 */
	 Directory createIndex(Document d1) throws IOException {
	        Directory directory = new RAMDirectory();
	        Analyzer analyzer = new SimpleAnalyzer(Version.LUCENE_40);
	        IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_40,
	                analyzer);
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
