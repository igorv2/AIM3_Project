package aim3.project.utils.kernels.vectorKernels;

import aim3.project.utils.IO.PactSVMVector;

public class PolynomialKernel implements VectorKernel {

	double offset;
	int degree;
	
	public PolynomialKernel(double offset, int degree) {
		super();
		this.offset = offset;
		this.degree = degree;
	}

	@Override
	public double calculate(PactSVMVector vector1, PactSVMVector vector2) {
		return Math.pow(vector1.computeScalarProduct(vector2)+offset,degree);
	}
	
	public double calculateUsingPrecomputedScalarProduct(double scalarProduct){
		return Math.pow(scalarProduct+offset,degree);
	}
}
