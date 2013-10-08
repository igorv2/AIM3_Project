package aim3.project.utils.kernels.vectorKernels;

import aim3.project.utils.IO.PactSVMVector;

public class CauchyKernel implements VectorKernel{
	double sigma;
	public CauchyKernel(double sigma) {
		super();
		this.sigma = sigma;
	}
	@Override
	public double calculate(PactSVMVector vector1, PactSVMVector vector2) {
		double distance=vector1.computeEuclidianDistance(vector2);
		return calculateUsingPrecomputedDistance(distance);
	}
	public double calculateUsingPrecomputedDistance(double distance){
		return 1/(1+(distance/sigma));
	}
}
