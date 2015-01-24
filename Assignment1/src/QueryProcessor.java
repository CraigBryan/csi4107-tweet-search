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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.*;
import org.apache.lucene.util.Version;
import java.io.File;
import java.util.ArrayList;

public class QueryProcessor {

	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args){
		
		
		try {
			
			//Standard Lucene intialization ''
			
			@SuppressWarnings("deprecation")
			StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_40);
			Directory index = new RAMDirectory();
			
			IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_40, analyzer);
			
			
			IndexWriter w;
			// ''
			
			// initialize index
			w = new IndexWriter(index, config);
			// add the tweets to the Lucene index 
			parseTweets(w, "res/input_tweets.txt");
			w.close();
			
			
			// retrieve the queries from the text file
			ArrayList<QueryXml> queries = retrieveQueriesFromTextFile("res/test_queries.txt");
			
			// initialize search tools
		    int hitsPerPage = 2;
		   // IndexReader reader = DirectoryReader.open(index);
		 //   IndexSearcher searcher = new IndexSearcher(reader);
		   // TopScoreDocCollector collector = TopScoreDocCollector.create(hitsPerPage, true);
		    
		    // prepare File Writer
		    
		    PrintWriter p = new PrintWriter(new FileWriter(new File("res/results.txt")));
		    
			for( QueryXml queryXml : queries){
				@SuppressWarnings("deprecation")
				
				//attempting to deal with bug
				IndexReader reader = DirectoryReader.open(index);
			    IndexSearcher searcher = new IndexSearcher(reader);
			    TopScoreDocCollector collector = TopScoreDocCollector.create(hitsPerPage, true);
			    
			    
				Query q = new QueryParser(Version.LUCENE_40, "tweet" , analyzer ).parse(queryXml.title);
				
				searcher.search(q, collector);						// it may be smarter to use a different method
			    ScoreDoc[] hits = collector.topDocs().scoreDocs;	// bug occurring
			    int rank = 1;
			    // add results to file
			    for(ScoreDoc hit: hits){
			    	Document d = searcher.doc(hit.doc);			   // may be bugged
			    	String id = d.get("id");
			    	String message = queryXml.num + "\t" + "Q0" + "\t" + id + "\t" + rank + "\t" + hit.score + "\t" + "testRun"; 
			    	p.println(message);
			    	p.flush();
			    	rank++;
			    }
			    
			}
			
			p.close();
			
			
			
		} catch (IOException e) {
			// deals with indexWriter constructor and parseTweets
			e.printStackTrace();
		} catch (Exception e) {
			// I'm not sure if the retrieveQueries(xml) method will only through SAXExceptions
			e.printStackTrace();
		}
	
		
	}

	
	/**
	 * 
	 * @param writer
	 * @param fileName
	 * @throws IOException
	 */
	private static void parseTweets(IndexWriter writer, String fileName) throws IOException{
		BufferedReader in = new BufferedReader(new FileReader(new File(fileName)));
	    String tweet;
	    // continuously parse tweets
		while((tweet = in.readLine()) != null){
			
			// add the tweet to the writer
			Document doc = new Document();
			try{
			String[] idAndMessage = tweet.split("\t"); // regular expression for tab
			doc.add(new TextField("tweet", idAndMessage[1], Store.YES));
			doc.add(new StringField("id", idAndMessage[0], Store.YES));
			writer.addDocument(doc);
			}
			catch(Exception e){
				System.out.println(tweet);
			}
			//
	    }
	
	}
	
	/**
	 * 
	 * @param fileName
	 * @throws IOException 
	 */
	private static ArrayList<QueryXml> retrieveQueriesFromTextFile(String fileName) throws IOException{
		
		ArrayList<QueryXml> queries = new ArrayList();
		BufferedReader in = new BufferedReader(new FileReader(new File(fileName)));
	    String line;
	    // continuously parse tweets
	    QueryXml query = new QueryXml();
	    boolean open = false;
		while((line = in.readLine()) != null){
			if(line.startsWith("<top>") || line.startsWith("</top>")){
				open = !open;
				
				if(open){	
					query = new QueryXml();
				}
				else{
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
		return queries;
	}
}
