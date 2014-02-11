import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

import org.apache.hadoop.io.Writable;


public class TpacfCartesianReader {

	private static final double D2R = (Math.PI/180.0);
	
	public CartesianArrayWritable readFile(String fileName) throws IOException {

		Scanner in;

		in = new Scanner(new FileReader(fileName));
		
		ArrayList<Writable> arry = new ArrayList<Writable>();
		
		try {
			while (in.hasNextDouble()) {
				double ra = in.nextDouble();
				double dec = in.nextDouble();
				
				double rarad = (double) D2R * ra;
				double decrad = (double) D2R * dec;
				double cd = (double) Math.cos(decrad);
				
				double x = (double) (Math.cos(rarad) * cd);
				double y = (double) (Math.sin(rarad) * cd);
				double z = (double) Math.sin(decrad);
				
				arry.add(new Cartesian(x, y, z));
			}
			CartesianArrayWritable data = new CartesianArrayWritable();
			data.set(arry.toArray(new Writable[0]));

			in.close();
			
			return data;
			
		} catch (NumberFormatException e) {

			in.close();
			
			throw new IOException("Error parsing floating point value.");
		}
		
	}
}
