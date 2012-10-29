package brickhouse.analytics.uniques;

/**
 *   Interface for counting distinct items,
 *    so that different implementations can be compared
 *     and substituted.
 *     
 *
 */
public interface ICountDistinct {
	
	public void addItem(String item);
	
	public double estimateReach();

	
}
