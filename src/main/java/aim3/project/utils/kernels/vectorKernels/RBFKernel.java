package aim3.project.utils.kernels.vectorKernels;

import aim3.project.utils.IO.PactSVMVector;

public class RBFKernel implements VectorKernel {

	double sigma;
	
	public RBFKernel(double sigma) {
		super();
		this.sigma=sigma;
	}

	@Override
	public double calculate(PactSVMVector vector1, PactSVMVector vector2) {
		return Math.exp(-(vector1.computeEuclidianDistance(vector2)/(2*sigma*sigma)));
	}
	
	public double calculateUsingPrecomputedDistance(double distance){
		return Math.exp(-(distance/(2*sigma*sigma)));
	}
}
