import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Tpacf extends Configured implements Tool {

	private static final int binsPerDec = 5;
	private static final double minArcmin = 1.0;
	private static final double maxArcmin = 10000.0;
	private final static int nbins  = (int) Math.floor(binsPerDec * (Math.log10(maxArcmin) - Math.log10(minArcmin)));

	private static final double D2R = Math.PI/180.0;
	
	public static class LongArrayWritable extends ArrayWritable {
		public LongArrayWritable() {
			super(LongWritable.class);
		}
		public LongArrayWritable(LongWritable[] values) {
	        super(LongWritable.class, values);
	    }
		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			Writable[] ws = get();
	        for (Writable w : ws) {
	        	String l = ((LongWritable) w).toString();
	            sb.append(l).append(" ");
	        }
	        return sb.toString();
		}
	}

	public static class ComputeMap extends Mapper<Text, TupleWritable, IntWritable, LongArrayWritable> {
		private final double [] binb = new double[nbins + 1];
    	private final static TpacfCompute c = new TpacfCompute();
		
    	private final static IntWritable RR = new IntWritable(0);
    	private final static IntWritable DR = new IntWritable(1);
    	
        @Override
        protected void map(Text key, TupleWritable pair, Context context)
                throws IOException, InterruptedException {
        	for (int k = 0; k < nbins+1; k++) {
    			binb[k] = Math.cos(Math.pow(10, Math.log10(minArcmin) +  k*1.0/binsPerDec) / 60.0*D2R);
    		}

        	CartesianArrayWritable data0 = (CartesianArrayWritable) pair.get(0);
        	CartesianArrayWritable data1 = (CartesianArrayWritable) pair.get(1);
        	
        	Cartesian[] random = (Cartesian[]) data0.toArray();
        	Cartesian[] data = (Cartesian[]) data1.toArray();


        	long [] rr = new long[nbins + 2];
        	long [] dr = new long[nbins + 2];
        	
        	for (int ii = 0; ii < random.length - 1; ii++) {
        		Cartesian d0 = random[ii];
            	for (int jj = ii + 1; jj < random.length; jj++) {
            		Cartesian d1 = random[jj];
            		double dot = d0.getX() * d1.getX() +
            					 d0.getY() * d1.getY() +
            					 d0.getZ() * d1.getZ();
                	int k = c.findK(dot, nbins, binb);
                	rr[k]++;
            	}
        	}

        	for (int ii = 0; ii < data.length; ii++) {
        		Cartesian d0 = data[ii];
            	for (int jj = 0; jj < random.length; jj++) {
            		Cartesian d1 = random[jj];
            		double dot = d0.getX() * d1.getX() +
            					 d0.getY() * d1.getY() +
            					 d0.getZ() * d1.getZ();
                	int k = c.findK(dot, nbins, binb);
                	dr[k]++;
            	}
        	}

        	LongWritable[] drw = new LongWritable[nbins+2];
        	LongWritable[] rrw = new LongWritable[nbins+2];
        	        	
        	for (int ii = 0; ii < nbins + 2; ii++) {
        		drw[ii] = new LongWritable(dr[ii]);
        		rrw[ii] = new LongWritable(rr[ii]);
        	}
        	
        	LongArrayWritable drWritable = new LongArrayWritable(drw);
        	LongArrayWritable rrWritable = new LongArrayWritable(rrw);
        	
        	
        	context.write(RR, rrWritable);
        	context.write(DR, drWritable);
        }
	}
	
    public static class Reduce extends Reducer<IntWritable, LongArrayWritable, IntWritable, LongArrayWritable> {

        @Override
        protected void reduce(IntWritable key, Iterable<LongArrayWritable> values, Context context)
                throws IOException, InterruptedException {
        	
        	Writable[] partialHistoWritable;
        	long [] histo = new long[nbins];
        	
			for (LongArrayWritable val : values) {
				partialHistoWritable = val.get();
        		for (int ii = 0; ii < nbins; ii++) {
        			histo[ii] += ((LongWritable) partialHistoWritable[ii]).get();
        		}
        	}
			
			LongWritable[] histoWritable = new LongWritable[nbins];
			for (int ii = 0; ii < nbins; ii++) {
				histoWritable[ii] = new LongWritable(histo[ii]);
			}
			
			LongArrayWritable res = new LongArrayWritable(histoWritable);
        	context.write(key, res);
        }
    }

	public int run(String[] arg0) throws Exception {
		
		Configuration conf = getConf();
        conf.set("DataInput", "/tmp/input/Datapnts.1");
        
        Job job = new Job(conf);
        job.setJarByClass(Tpacf.class);
        job.setMapperClass(ComputeMap.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        
        System.out.println(job.getJar());
        
        job.setInputFormatClass(TpacfReader.class);
        
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(LongArrayWritable.class);

        TpacfReader.setInputPaths(job, new Path("/tmp/input/Randompnts.*"));
        
        TextOutputFormat.setOutputPath(job, new Path("/tmp/out"));
        
        return job.waitForCompletion(true) ? 0 : 1;
    }
    
    public static void main(String[] args) throws Exception {
    	try {
	        int result = ToolRunner.run(new Configuration(), new Tpacf(), args);;
	        System.exit(result);
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    }
}