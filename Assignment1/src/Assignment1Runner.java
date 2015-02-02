import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Assignment1Runner {
    
    public String DATA_FOLDER = "res/";
    public String LIB_FOLDER = "lib/";
    public String INPUT_FILE = "input_tweets.txt";
    public String QUERIES_FILE = "test_queries.txt";
    public String OUTPUT_FILE = "results.txt";
    public String RELEVANCE_FEEDBACK_FILE = "Trec_microblog11-qrels.txt";
    public String EVALUATION_RESULT_FILE = "eval_results.txt";

    public Assignment1Runner() {
    	//Empty
    }

    public void indexAndSearch() {
    	QueryProcessor.go();
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
    		System.out.println(pr.waitFor());
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
    		pr = rt.exec("./" + LIB_FOLDER + "trec_eval.8.1/trec_eval " + DATA_FOLDER + RELEVANCE_FEEDBACK_FILE + " " + DATA_FOLDER + OUTPUT_FILE);
    		
    		BufferedReader input = new BufferedReader(new InputStreamReader(pr.getInputStream()));
   		 
            String line=null;

            while((line=input.readLine()) != null) {
                System.out.println(line);
            }
            
    		System.out.println(pr.waitFor());
    		
    	} catch(IOException e) {
    		System.out.println("IOException during trec_eval");
    		e.printStackTrace();
    	} catch(InterruptedException e) {
    		System.out.println("InterruptedException during trec_eval");
    		e.printStackTrace();
    	}
    }
}