package aim3.project.utils.kernels.vectorKernels;

import aim3.project.utils.IO.PactSVMVector;

public class ExponentialKernel implements VectorKernel {
	
	double sigma;
	
	public ExponentialKernel(double sigma) {
		super();
		this.sigma = sigma;
	}

	@Override
	public double calculate(PactSVMVector vector1, PactSVMVector vector2) {
		return Math.exp(-(Math.sqrt(vector1.computeEuclidianDistance(vector2))/(2*sigma*sigma)));
	}
	
	public double calculateUsingPrecomputedDistance(double distance){
		return Math.exp(-(Math.sqrt(distance)/(2*sigma*sigma)));

	}

}
