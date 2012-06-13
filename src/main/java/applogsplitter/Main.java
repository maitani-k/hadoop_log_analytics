package applogsplitter;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;

public class Main {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		
		Job job = JobFactory.getApplogSplitJob(conf, Main.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		boolean success = job.waitForCompletion(true);
		System.out.println(success);
	}
}
