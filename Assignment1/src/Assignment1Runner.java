import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Arrays;

/* This class runs the tweet searching with the input tweets and 
 * the queries. It takes a few command line arguments.
 */
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

    private boolean useRelevanceFeedback;
    private Double oQCoef;
    private Double rQCoef;
    private Double iQCoef;
    private Integer numR;
    private boolean useHashtagScoring;
    private Float htCoef;
    private static boolean noEval;
    private QueryProcessor.AnalyzerChoice ac;

    public Assignment1Runner(String[] args) {
    	parseCommandLineArguments(args);
    }

    // Does non-evaluation tasks (building the index, parsing queries, and
    // searching for results)
    public void indexAndSearch() {
    	Double[] relevanceArray = new Double[4];
    	relevanceArray[0] = oQCoef;
    	relevanceArray[1] = rQCoef;
    	relevanceArray[2] = iQCoef;
    	QueryProcessor q = new QueryProcessor(DATA_FOLDER + INPUT_FILE, 
    										  DATA_FOLDER + QUERIES_FILE,
                                              DATA_FOLDER + VOCAB_OUTPUT_FILE, 
    										  DATA_FOLDER + OUTPUT_FILE,
                                              useRelevanceFeedback,
                                              relevanceArray,
                                              numR,
                                              useHashtagScoring,
                                              htCoef,
                                              ac);
    	q.go();
    }

    // Compiles and uses the trec_eval script to evaluate the results
    public void evaluate() {
        compileTrecEval();
        runTrecEval();
    }

    public static void main(String[] args) {
        Assignment1Runner runner = new Assignment1Runner(args);

        runner.indexAndSearch();

        if(!noEval) runner.evaluate();
        
        System.out.println("Done!");
    }

    private void compileTrecEval() {
        
        //Skips compilation if already compiled
        File f = new File(LIB_FOLDER + "trec_eval.8.1/trec_eval");
        if(f.exists()) {
            return;
        }

    	Runtime rt = Runtime.getRuntime();
    	Process pr;
    	try {
    		pr = rt.exec("make -C " + LIB_FOLDER + "trec_eval.8.1");
    		int result = pr.waitFor();
    		
    		if (result != 0) throw new IOException("trec_eval exited with exit code" 
                + String.valueOf(result));
    		
    	} catch(IOException | InterruptedException e) {
    		System.out.println("Trec_eval could not be compiled. " +
                "The result files will still be generated.\n" + 
                "Evaluation will need to be done manually, sorry. " +
                "See res/results.txt for the results.");
    	} 
    }
    
    private void runTrecEval() {
    	Runtime rt = Runtime.getRuntime();
    	Process pr;
    	try {
            //The kinda complex trec_eval command line call
    		pr = rt.exec("./" + 
                         LIB_FOLDER + 
                         "trec_eval.8.1/trec_eval " + 
                         TREC_ARGUMENTS + 
                         " " + 
                         DATA_FOLDER + 
                         RELEVANCE_FEEDBACK_FILE + 
                         " " + 
                         DATA_FOLDER + 
                         OUTPUT_FILE);
    		
    		saveResultToFile(DATA_FOLDER + 
                             EVALUATION_RESULT_FILE, 
                             pr.getInputStream());
            
    		int result = pr.waitFor();
    		if (result != 0) 
                throw new IOException("trec_eval exited with exit code " + 
                                      String.valueOf(result));
    		
    	} catch(IOException | InterruptedException e) {
    		System.out.println("Trec_eval could not be run. " + 
                "Result files will still be generated.\n" + 
                "Evaluation will need to be done manually, sorry. " +
                "See res/results.txt for the results.");
    	}
    }
        
    // Saves the eval results to file
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

    // Parses any command line arguments
    private void parseCommandLineArguments(String[] args) {
        
        //Prints a help message and exits
        if(Arrays.asList(args).contains("-h")) {
            printHelp();
            System.exit(1);
        }

        //Relevance feedback option
        if(Arrays.asList(args).contains("-r")) {
            useRelevanceFeedback = true;
        } else {
            useRelevanceFeedback = false;
        }
        
        //Relevance feedback coefficients
        if(useRelevanceFeedback) {
            int index = Arrays.asList(args).indexOf("-oQCoef");
            if(index != -1) {
                try {
                    oQCoef = Double.valueOf(args[index + 1]);
                } catch(NumberFormatException | 
                    ArrayIndexOutOfBoundsException e) {
                    System.out.println("Improper value set for oQCoef " +
                        "please a number (double) value as the argument " +
                        "after -oQcoef. Default originalQueryCoefficient " +
                        "being used for relevance feedback");
                    oQCoef = null; 
                }
            }

            index = Arrays.asList(args).indexOf("-rQCoef");
            if(index != -1) {
                try {
                    rQCoef = Double.valueOf(args[index + 1]);
                } catch(NumberFormatException | 
                    ArrayIndexOutOfBoundsException e) {
                    System.out.println("Improper value set for rQCoef " +
                        "please a number (double) value as the argument " +
                        "after -rQcoef. Default relevantQueryCoefficient " +
                        "being used for relevance feedback");
                    rQCoef = null; 
                }
            }

            index = Arrays.asList(args).indexOf("-iQCoef");
            if(Arrays.asList(args).indexOf("-iQCoef") != -1) {
                try {
                    iQCoef = Double.valueOf(args[index + 1]);
                } catch(NumberFormatException | 
                    ArrayIndexOutOfBoundsException e) {
                    System.out.println("Improper value set for iQCoef " +
                        "please a number (double) value as the argument " +
                        "after -iQcoef. Default irrelevantQueryCoefficient " +
                        "being used for relevance feedback");
                    iQCoef = null; 
                }
            }

            index = Arrays.asList(args).indexOf("-numR");
            if(Arrays.asList(args).indexOf("-numR") != -1) {
                try {
                    numR = Integer.valueOf(args[index + 1]);
                } catch(NumberFormatException | 
                    ArrayIndexOutOfBoundsException e) {
                    System.out.println("Improper value set for numR " +
                        "please a number (integer) value as the argument " +
                        "after -numR. Default irrelevantQueryCoefficient " +
                        "being used for relevance feedback");
                    numR = null; 
                }
            }
        }

        //Hashtag scoring option
        if(Arrays.asList(args).contains("-t")) {
            useHashtagScoring = true;
        } else {
        	useHashtagScoring = false;
        }
        
        // Hashtag scoring coefficient 
        if(useHashtagScoring) {
            int index = Arrays.asList(args).indexOf("-htCoef");
            if(index != -1) {
                try {
                	htCoef = Float.valueOf(args[index + 1]);
                } catch(NumberFormatException | 
                    ArrayIndexOutOfBoundsException e) {
                    System.out.println("Improper value set for htCoef " +
                        "please a number (double) value as the argument " +
                        "after -htCoef. Default hashtagScoreCoefficient " +
                        "being used for hashtag scoring feedback");
                    oQCoef = null; 
                    System.exit(0);
                }
            }
        }
        
        //NoEval option
        if(Arrays.asList(args).contains("-n")) {
            noEval = true;
        } else {
            noEval = false;
        }
        
        //Analyzer options
        if(Arrays.asList(args).contains("-e")) {
        	ac = QueryProcessor.AnalyzerChoice.ENGLISH;
        } else {
        	ac = QueryProcessor.AnalyzerChoice.STANDARD;
        }
    }

    // Help command line output
    private void printHelp() {
        System.out.println("This is the command line interface. The " +
            "command line options are below:\n " +
            "\t-h - prints this message\n" + 
            "\t-r - use relevance feedback\n" +
            "\t-t - enable hashtag scoring\n" +
            "\t-n - do not perform evaluation (creates " + 
            "results and vocabulary file only)\n" +
            "\t-e - use the EnglishAnalyzer rather than Lucene's " +
            "StandardAnalyzer\n" +
            "\t-oQCoef VAL - sets the originalQueryCoefficient to VAL\n" +
            "\t-rQCoef VAL - sets the relevantQueryCoefficient to VAL\n" +
            "\t-iQCoef VAL - sets the irrelevantQueryCoefficient to VAL\n" +
            "\t-numR VAL - sets the number of relevant documents to consider" +
            "for relevance feeback\n" + 
            "\t-htCoef VAL - sets the hashtagScoreCoefficient to VAL\n\n" +
            "There are a few required input files. They need to go in the " +
            "/res folder. The files required there are as follows:\n" +
            "\tinput_tweets.txt - the input tweets to be searched\n" +
            "\ttest_queries.txt - the queries to be performed\n" +
            "\tTrec_microblog11-qrels.txt - the query relevance answer file. " +
            "\tTHESE ARE REQUIRED FOR EVALUATION\n\n" +
            "Outputs appear in the /res folder. The following files will " + 
            "be created there\n" + 
            "\tvocabulary.txt - Data about the vocabulary used (number of " +
            "terms and 100 random terms from the vocabulary\n" +
            "\tresults.txt - The searching results. 1000 documents for " + 
            "each query\n\teval_results.txt - EVALUATION ONLY. This file " +
            "contains the results of running trec_eval on the results.");
    }
}