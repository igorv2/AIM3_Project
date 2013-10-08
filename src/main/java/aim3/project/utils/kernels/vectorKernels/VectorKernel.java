package aim3.project.utils.kernels.vectorKernels;

import aim3.project.utils.IO.PactSVMVector;

/**
 * The interface for all kernels working on vectors
 * @author igorv
 *
 */

public interface VectorKernel {
	
	/**
	 * A method which takes two vectors and calculates their kernel function
	 * @param vector1 
	 * @param vector2
	 * @return k(vector1, vector2)
	 */
	public abstract double calculate(PactSVMVector vector1, PactSVMVector vector2);
}
