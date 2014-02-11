import java.util.ArrayList;


public class TpacfCompute {

	public void compute(ArrayList<Cartesian> data, int nbins, double[] binb, long[] dataBins) {
		compute(data, data, nbins, binb, dataBins);
		return ;
	}
	
	public void compute(ArrayList<Cartesian> data1, ArrayList<Cartesian> data2, int nbins, double[] binb, long[] dataBins) {
		boolean doSelf = data1 == data2;
		int end = doSelf ? data1.size() - 1 : data1.size();
		
		for (int ii = 0; ii < end; ii++) {
			Cartesian elemI = data1.get(ii);
			double xi = elemI.getX();
			double yi = elemI.getY();
			double zi = elemI.getZ();

			int start = doSelf ? ii + 1 : 0;
			for (int jj = start; jj < data2.size(); jj++) {
				Cartesian elemJ = data2.get(jj);
				double xj = elemJ.getX();
				double yj = elemJ.getY();
				double zj = elemJ.getZ();


				double dot = xi * xj + yi * yj + zi * zj;
				int k = findK(dot, nbins, binb);
				
				dataBins[k]++;
				
			}
		}
		return ;
	}
	
	public int findK(double dot, int nbins, double [] binb) {
		int min = 0;
		int max = nbins;
		int k;

		while (max > (min+1)) {
			k = (min + max) / 2;
			if (dot >= binb[k]) 
				max = k;
			else 
				min = k;
		}
		
		if (dot >= binb[min]) {
			k = min; 
		} else if (dot < binb[max]) { 
			k = max+1; 
		} else  { 
			k = max; 
		}
		
		return k;
	}
}
