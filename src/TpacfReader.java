import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;



public class TpacfReader extends FileInputFormat<Text, TupleWritable> {
	
	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		return false;
	}
	
	@Override
	public RecordReader<Text, TupleWritable> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException, InterruptedException {
		String dataInput = context.getConfiguration().get("DataInput");
		TpacfCartesianReader reader = new TpacfCartesianReader();
		CartesianArrayWritable w = reader.readFile(dataInput);
		return new TpacfCartesianRecordReader(w);
	}
}
