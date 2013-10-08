package aim3.project.utils.IO;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

import eu.stratosphere.pact.common.type.Value;

/**
 * An immutable vector implementation using the PactValue interface. It supports
 * both sparse and dense vectors, and can alternate between the two formats to
 * save space.
 * 
 * WARNING: This can result in dense vectors of lower dimensionality, as the
 * trailing zeros won't be serialized. As the vectors are only read throughout
 * the code, this serves to reduce network transfer when possible
 * 
 * When serialized, its format is: for sparse: <(byte)0><length><index,
 * Value><index, Value>...<index, Value> for dense vectors:
 * <(byte)1><length><value><value>...<value>
 * 
 * @author Igor Viskovic
 * 
 */

public class PactSVMVector implements Value {

	boolean sparse; // Indicator for sparsity
	int[] nonZero; // In case the vector is sparse, the indicies of non-zero
					// elements are saved here
	double[] values; // The array containing the actual values
	SparseVectorEntry[] sparseRepresentation;

	@Override
	public void write(DataOutput out) throws IOException {

		if (sparse) {
			out.writeByte(0);
		} else {
			out.writeByte(1);
		}
		out.writeInt(values.length);
		if (sparse) {
			for (int i = 0; i < values.length; i++) {
				out.writeInt(sparseRepresentation[i].getIndex());
				out.writeDouble(sparseRepresentation[i].getValue());
			}
		} else {
			for (int i = 0; i < values.length; i++) {
				out.writeDouble(values[i]);
			}
		}
	}

	@Override
	public void read(DataInput in) throws IOException {
		Byte b = in.readByte();
		if (b > 0)
			sparse = true;
		else
			sparse = false;
		int length = in.readInt();
		int countNonZero = 0; // Indicator of how many elements of the vector
								// are !=0
		if (length == 0) {
			sparse = false;
			values = new double[1];
			// We store the zero vector as a single a dense vector of length one
			// with a single 0
			return;
		}
		if (sparse) {
			sparseRepresentation = new SparseVectorEntry[length];
			for (int i = 0; i < length; i++) {
				sparseRepresentation[i] = new SparseVectorEntry(in.readInt(),
						in.readDouble());
			}

		} else {
			values = new double[length];
			nonZero = null;
			for (int i = 0; i < length; i++) {
				values[i] = in.readDouble();
				countNonZero += ((values[i] != 0) ? 1 : 0);
			}
			if (countNonZero == 0) {
				sparse = false;
				values = new double[1];
				// We store the zero vector as a single a dense vector of length
				// one with a single 0
				return;
			}
			if (countNonZero <= (length / 5) && (length > 6)) {
				// The vector should be saved as a sparse vector to save space
				sparse = true;
				sparseRepresentation = new SparseVectorEntry[countNonZero];
				for (int i = 0, j = 0; i < length; i++) {
					if (values[i] != 0) {
						sparseRepresentation[j++] = new SparseVectorEntry(i,
								values[i]);
					}
				}
				values = null;

			}
		}
	}

	public int getIndex(int lookupMarker) {
		if (sparse)
			return ((lookupMarker < values.length && lookupMarker >= 0)) ? lookupMarker
					: -1;
		else
			return (lookupMarker < values.length && lookupMarker >= 0) ? nonZero[lookupMarker]
					: -1;
	}

	public double getValueForIndex(int index) {
		if (index >= 0 && index < values.length)
			return values[index];
		else
			return 0;
	}

	public SparseVectorEntry getPairForIndex(int index) {
		if (sparse && (index >= 0 && index < values.length))
			return sparseRepresentation[index];
		else if (index >= 0 && index < values.length)
			return new SparseVectorEntry(index, values[index]);
		else
			return null;
	}

	public boolean isSparse() {
		return sparse;
	}

	public int getLength() {
		if (sparse)
			return sparseRepresentation.length;
		else
			return values.length;
	}

	public double computeEuclidianDistance(PactSVMVector vector) {
		double ret = 0;

		int index1 = 0; // The position in this array we're currently at
		int index2 = 0; // The position in the other vector's array

		SparseVectorEntry ourEntry = getPairForIndex(index1);
		SparseVectorEntry otherEntry = vector.getPairForIndex(index2);
		int length1 = getLength();
		int length2 = vector.getLength();

		while (index1 < length1 && index2 < length2) {
			if (ourEntry.getIndex() == otherEntry.getIndex()) {
				// Both are in the same position
				ret += Math.pow((ourEntry.getValue() - otherEntry.getValue()),
						2);
				ourEntry = getPairForIndex(++index1);
				otherEntry = vector.getPairForIndex(++index2);
			} else if (ourEntry.getIndex() < otherEntry.getIndex()) {
				ret += Math.pow(ourEntry.getValue(), 2);
				ourEntry = getPairForIndex(++index1);
			} else {
				ret += Math.pow(otherEntry.getValue(), 2);
				otherEntry = vector.getPairForIndex(++index2);
			}
		}
		while (index1 < length1) {
			ret += Math.pow(ourEntry.getValue(), 2);
			ourEntry = getPairForIndex(++index1);
		}
		while (index2 < length2) {
			ret += Math.pow(otherEntry.getValue(), 2);
			otherEntry = vector.getPairForIndex(++index2);
		}

		return ret;
	}

	public double computeScalarProduct(PactSVMVector vector) {
		double ret = 0;

		int index1 = 0; // The position in this array we're currently at
		int index2 = 0; // The position in the other vector's array

		SparseVectorEntry ourEntry = getPairForIndex(index1);
		SparseVectorEntry otherEntry = vector.getPairForIndex(index2);
		int length1 = getLength();
		int length2 = vector.getLength();

		while (index1 < length1 && index2 < length2) {
			if (ourEntry.getIndex() == otherEntry.getIndex()) {
				// Both are in the same position
				ret += ourEntry.getValue() * otherEntry.getValue();
				ourEntry = getPairForIndex(++index1);
				otherEntry = vector.getPairForIndex(++index2);
			} else if (ourEntry.getIndex() < otherEntry.getIndex()) {
				ourEntry = getPairForIndex(++index1);
			} else {
				otherEntry = vector.getPairForIndex(++index2);
			}
		}
		return ret;
	}

	public double[] componentWiseProducts(PactSVMVector vector) {
		double[] ret = null;
		LinkedList<Double> list = new LinkedList<Double>();
		int index1 = 0; // The position in this array we're currently at
		int index2 = 0; // The position in the other vector's array

		SparseVectorEntry ourEntry = getPairForIndex(index1);
		SparseVectorEntry otherEntry = vector.getPairForIndex(index2);
		int length1 = getLength();
		int length2 = vector.getLength();

		while (index1 < length1 && index2 < length2) {
			if (ourEntry.getIndex() == otherEntry.getIndex()) {
				// Both are in the same position
				list.add(ourEntry.getValue() * otherEntry.getValue());
				ourEntry = getPairForIndex(++index1);
				otherEntry = vector.getPairForIndex(++index2);
			} else if (ourEntry.getIndex() < otherEntry.getIndex()) {
				ourEntry = getPairForIndex(++index1);
			} else {
				otherEntry = vector.getPairForIndex(++index2);
			}
		}
		if (list.size() > 0) {
			Iterator<Double> it = list.iterator();
			ret = new double[list.size()];
			int i = 0;
			while (it.hasNext()) {
				ret[i] = it.next();
			}
			return ret;
		} else
			return new double[1];
	}

}
