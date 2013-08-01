package aim3.project;

public class NGramKernel  {
	int minn;
	int maxn;
	int w;
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
	public int calculate(String x, String y){
		int r=0;
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
				r+=(j*w)*c;
			}
		
		
		return r;
	}
	

}
