package aim3.project.utils.kernels.vectorKernels;

import aim3.project.utils.IO.PactSVMVector;

public class InverseMultiquadricKernel implements VectorKernel {
	double offset;
	
	
	public InverseMultiquadricKernel(double offset) {
		super();
		this.offset = offset;
	}


	@Override
	public double calculate(PactSVMVector vector1, PactSVMVector vector2) {
		return Math.pow((vector1.computeEuclidianDistance(vector2)+offset),-0.5);
	}
	
	public double calculateUsingPrecomputedDistance(double distance){
		return Math.pow(distance+offset,-0.5);
	}
	
}
