import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class Cartesian implements Writable, WritableComparable<Object> {
	private double x, y, z;


	public Cartesian() {
		x = y = z = 0;
	}
	
	public Cartesian(double x, double y, double z) {
		this.x = x;
		this.y = y;
		this.z = z;
	}
	
	public double getX() {
		return x;
	}

	public void setX(double x) {
		this.x = x;
	}

	public double getY() {
		return y;
	}

	public void setY(double y) {
		this.y = y;
	}

	public double getZ() {
		return z;
	}

	public void setZ(double z) {
		this.z = z;
	}
	
	public String toString() {
		String str = "(";
		str += x;
		str += ", ";
		str += y;
		str += ", ";
		str += z;
		str += ")";
		
		return str;
	}

	public void write(DataOutput out) throws IOException {
		out.writeDouble(x);
		out.writeDouble(y);
		out.writeDouble(z);
	}

	public void readFields(DataInput in) throws IOException {
		x = in.readDouble();
		y = in.readDouble();
		z = in.readDouble();
	}

	public static Cartesian read(DataInput in) throws IOException {
		Cartesian w = new Cartesian();
		w.readFields(in);
		return w;
	}
	
	public double squareDistanceFromOrigin() {
		return x*x + y*y + z*z;
	}


	public int compareTo(Object arg0) {
		Cartesian other = (Cartesian) arg0;
		return Double.compare(squareDistanceFromOrigin(),
				  other.squareDistanceFromOrigin());
	}

}
