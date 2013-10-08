package aim3.project.utils.kernels.vectorKernels;

import aim3.project.utils.IO.PactSVMVector;

public class RationalQuadraticKernel implements VectorKernel {

	double offset;
	
	public RationalQuadraticKernel(double offset) {
		super();
		this.offset=offset;
	}

	@Override
	public double calculate(PactSVMVector vector1, PactSVMVector vector2) {
		double distance=vector1.computeEuclidianDistance(vector2);
		return calculateUsingPrecomputedDistance(distance);
	}
	
	public double calculateUsingPrecomputedDistance(double distance){
		return 1.0-(distance)/(distance+offset);
	}
	
}
