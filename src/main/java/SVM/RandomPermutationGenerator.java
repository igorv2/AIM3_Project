package SVM;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Random;


public class RandomPermutationGenerator {
	int length;
	long seed;
	int start;
	int stop;
	int[] permutation;
	HashMap<Integer, Integer> map;

	/**
	 * Generates the initial array for the random permutation
	 */
	private void generateArray() {
		permutation = new int[length];
		for (int i = 0; i < length; i++) {
			permutation[i] = i;
		}
	}

	/**
	 * Uses the Fisher-Yates shuffle to create a permutation of the array
	 */
	public void shuffle() {
		Random r = new Random(seed);
		int t = 0;
		int ind = 0;
		for (int i = 0; i < length; i++) {
			ind = r.nextInt(i + 1);
			t = permutation[ind];
			permutation[ind] = permutation[i];
			permutation[i] = t;
		}
		map=new HashMap<Integer, Integer>();
		for (int i = 0; i < length; i++) {
			if (permutation[i] >= start && permutation[i] < stop)
				map.put(i, permutation[i]);
		}
	}

	/**
	 * Creates the reproducible random permutation, containing an array of
	 * indices
	 * 
	 * @param l
	 *            The length of the array (number of items)
	 * @param s
	 *            The initial seed
	 */

	public RandomPermutationGenerator(int length, long seed, int start, int stop) {
		this.length = length;
		this.seed = seed;
		this.start=start;
		this.stop=stop;
		generateArray();
		shuffle();
	}

	/**
	 * Returns the shuffled value or -1, if it doesn't fit in the desired
	 * interval
	 * 
	 * @param indexOfTheElement
	 *            The index in the original dataset. In [0,n)
	 * @return The value after in the desired group, or -1 if it's not in it
	 */
	public int getValueForIndex(int indexOfTheElement) {
		int ind=indexOfTheElement%length; // Just to account for indicies [1,n]

		if(map.containsKey(ind))
			return map.get(ind);
		
		return -1;
	}

	public void print() {
		String s = "";
		for (int i = 0; i < length; i++) {
			s = s + permutation[i];
			s = s + " ";
			if (i % 5 == 4 && i != 0)
				s += '\n';
		}
		System.out.print(s);
	}

}
