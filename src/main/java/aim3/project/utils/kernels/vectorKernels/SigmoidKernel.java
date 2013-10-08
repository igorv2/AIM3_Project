package aim3.project.utils.kernels.vectorKernels;

import aim3.project.utils.IO.PactSVMVector;

public class SigmoidKernel implements VectorKernel {
	double alpha;
	double offset;
	
	@Override
	public double calculate(PactSVMVector vector1, PactSVMVector vector2) {
		return Math.tanh(alpha*vector1.computeScalarProduct(vector2)+offset);
	}

	public SigmoidKernel(double alpha, double offset) {
		super();
		this.alpha = alpha;
		this.offset = offset;
	}
	public double calculateUsingPrecomputedProduct(double product){
		return Math.tanh(alpha*product+offset);
	}
}
