import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

public class Assignment1Runner {
    
    public final String DATA_FOLDER = "res/";
    public final String LIB_FOLDER = "lib/";
    public final String INPUT_FILE = "input_tweets.txt";
    public final String QUERIES_FILE = "test_queries.txt";
    public final String OUTPUT_FILE = "results.txt";
    public final String VOCAB_OUTPUT_FILE = "vocabulary.txt";
    public final String RELEVANCE_FEEDBACK_FILE = "Trec_microblog11-qrels.txt";
    public final String EVALUATION_RESULT_FILE = "eval_results.txt";
    public final String TREC_ARGUMENTS = "-o";

    public Assignment1Runner() {
    	//Empty
    }

    public void indexAndSearch() {
    	QueryProcessor q = new QueryProcessor(DATA_FOLDER + INPUT_FILE, 
    										  DATA_FOLDER + QUERIES_FILE,
                                              DATA_FOLDER + VOCAB_OUTPUT_FILE, 
    										  DATA_FOLDER + OUTPUT_FILE);
    	q.go();
    }

    public void evaluate() {
        compileTrecEval();
        runTrecEval();
    }

    public static void main(String args[]) {
        Assignment1Runner runner = new Assignment1Runner();

        runner.indexAndSearch();
        runner.evaluate();
    }

    private void compileTrecEval() {
    	Runtime rt = Runtime.getRuntime();
    	Process pr;
    	try {
    		pr = rt.exec("make -C " + LIB_FOLDER + "trec_eval.8.1");
    		int result = pr.waitFor();
    		
    		if (result != 0) throw new IOException("trec_eval exited with exit code" + String.valueOf(result));
    		
    	} catch(IOException e) {
    		System.out.println("IOException during compiling");
    		e.printStackTrace();
    	} catch(InterruptedException e) {
    		System.out.println("InterruptedException during compiling");
    		e.printStackTrace();
    	}
    }
    
    private void runTrecEval() {
    	Runtime rt = Runtime.getRuntime();
    	Process pr;
    	try {
    		pr = rt.exec("./" + LIB_FOLDER + "trec_eval.8.1/trec_eval " + TREC_ARGUMENTS + " " + DATA_FOLDER + RELEVANCE_FEEDBACK_FILE + " " + DATA_FOLDER + OUTPUT_FILE);
    		
    		saveResultToFile(DATA_FOLDER + EVALUATION_RESULT_FILE, pr.getInputStream());
            
    		int result = pr.waitFor();
    		if (result != 0) throw new IOException("trec_eval exited with exit code " + String.valueOf(result));
    		
    	} catch(IOException e) {
    		System.out.println("IOException during trec_eval");
    		e.printStackTrace();
    	} catch(InterruptedException e) {
    		System.out.println("InterruptedException during trec_eval");
    		e.printStackTrace();
    	}
    }
    
    private void saveResultToFile(String filename, InputStream inputStream) {
    	
    	BufferedWriter output = null;
    	BufferedReader input = new BufferedReader(
				new InputStreamReader(inputStream));
    	
    	try {
		 	output = new BufferedWriter(
		 		new OutputStreamWriter(
		 		new FileOutputStream(filename), "utf-8"));
		 	
	        String line = null;
	
	        while((line = input.readLine()) != null) {
	            System.out.println(line);
	        	output.write(line + "\n");
	        }
	        
        } catch (IOException e) {
        	e.printStackTrace();
        } finally {
        	try{ output.close(); } catch (IOException e) {}
        }
    }
}