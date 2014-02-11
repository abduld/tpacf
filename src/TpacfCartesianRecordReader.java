import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class TpacfCartesianRecordReader extends RecordReader<Text, TupleWritable> {
	
	private int done = 0; 
	private String fileName;
	private TupleWritable currentValues;
	private Text key;


	private CartesianArrayWritable data;

	public TpacfCartesianRecordReader(CartesianArrayWritable data) {
		this.data = data;
	}
	
	public void initialize(InputSplit inputSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		
		currentValues = null;
		
		
		FileSplit split = (FileSplit) inputSplit;

        Path path = split.getPath();
        fileName = path.toUri().getPath().toString();
        
        System.out.println("filename = " + fileName);
        
        key = new Text(fileName);
	}

	@Override
	public void close() throws IOException {
		done = 1;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {

		
		if (done == 1) {
			return false;
		}
		
		Writable[] w = new Writable[2];
		
		TpacfCartesianReader reader = new TpacfCartesianReader();
		
		CartesianArrayWritable random = reader.readFile(fileName);
		
		w[0] = random;
		w[1] = data;
		
		currentValues = new TupleWritable(w);
		
		done = 1;
		
		return true;
	}

    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

	@Override
	public TupleWritable getCurrentValue() throws IOException, InterruptedException {
		return currentValues;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return done;
	}


}
