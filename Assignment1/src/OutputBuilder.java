import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;


public class OutputBuilder {

    BufferedWriter writer;
    int rank;

    public OutputBuilder(String fileName) {
    	try {
    		writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName)));
    	} catch (FileNotFoundException e) {
    		System.out.println("Error while writing to file:" + fileName);
    		e.printStackTrace();
    	}
    	
    	rank = 1;
    }

    public void add(String queryNum, String id, float score) {
        try {
        	writer.write(buildOutputString(queryNum, id, score));
        } catch (IOException e) {
        	System.out.println("Error while writing to output file");
        	e.printStackTrace();
        }
        rank++;
    }

    public void resetRank() {
        rank = 1;
    }

    public void close() {
        try {
        	writer.close();
        } catch (IOException e) {
        	
        }
    }

    private String buildOutputString(String queryNum, String id, float score) {
        return stripPrefix(queryNum) + "\tQ0\t" + id + "\t" + rank + "\t"
            + score + "\ttestRun\n";
    }

    private static String stripPrefix(String input) {
    	String output = input.substring(2);
        return String.valueOf(Integer.parseInt(output));
    }
}