package aim3.project.utils.kernels.stringKernels;

public class NGramKernel  {
	int minn;
	int maxn;
	double w;
	public NGramKernel(int minn, int maxn, int w) {
		super();
		this.minn = minn;
		this.maxn = maxn;
		this.w = w;
	}
	
	/**
	 * Calculates the value of the kernel for x and Y
	 * @param x: String
	 * @param y: String
	 * @return Integer with the weight
	 */
	public double calculate(String x, String y){
		double r=0;
		int i,j,k,c;
		
		for(i=0;i<x.length();i++)
			for(j=minn;j<=maxn && i+j<x.length();j++){
				String t=x.substring(i, i+j);
				boolean finished=false;
				k=0;
				c=0;
				while(!finished){
					k=y.indexOf(t, k); // Look for the given substring from position k onward 
					if(k!=-1){
						c++;
						k++;
					}
					else finished=true;
				}
				r+=c;
			}
		
		
		return r;
	}
	

}
