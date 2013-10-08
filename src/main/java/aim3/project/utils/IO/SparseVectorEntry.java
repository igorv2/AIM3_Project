package aim3.project.utils.IO;

public class SparseVectorEntry {
	int index;
	double value;
	public int getIndex() {
		return index;
	}
	public void setIndex(int index) {
		this.index = index;
	}
	public double getValue() {
		return value;
	}
	public void setValue(double value) {
		this.value = value;
	}
	public SparseVectorEntry(int index, double value) {
		super();
		this.index = index;
		this.value = value;
	}
	public SparseVectorEntry() {
		index=0;
		value=0;
	}
}
