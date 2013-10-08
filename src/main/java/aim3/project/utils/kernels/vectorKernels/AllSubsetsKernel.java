package aim3.project.utils.kernels.vectorKernels;

import aim3.project.utils.IO.PactSVMVector;

public class AllSubsetsKernel implements VectorKernel {
	
	@Override
	public double calculate(PactSVMVector vector1, PactSVMVector vector2) {
		double[]products=vector1.componentWiseProducts(vector2);
		double ret=1;
		for(int i=0;i<products.length;i++){
			ret*=(1+products[i]);
		}
		return ret;
	}

}
