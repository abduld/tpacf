import org.apache.hadoop.io.ArrayWritable;


public class CartesianArrayWritable extends ArrayWritable {
	public CartesianArrayWritable() {
		super(Cartesian.class);
	}
}