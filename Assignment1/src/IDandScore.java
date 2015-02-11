/**
 * Simple custom sortable object for storing scores with id's.
 * The objects are stored by score.
 */
public class IDandScore implements Comparable<IDandScore> {
	public String id;
	public Float score; 
	
	public IDandScore(String id, Float score) {
		this.id = id;
		this.score = score;
	}

	@Override
	public int compareTo(IDandScore other) {
		//Sorts in reverse order
		return (-1) * this.score.compareTo(other.score);
	}
	
	
}

