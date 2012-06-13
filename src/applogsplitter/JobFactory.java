package applogsplitter;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;

import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;

import applogsplitter.mapper.AppLogSeparatorMapper;
import applogsplitter.reducer.NoReducer;
import applogsplitter.writable.AccessLogWritable;

public class JobFactory {

	public static Job getApplogSplitJob(Configuration conf, Class<?> mainClass) throws IOException{
		
		MongoConfigUtil.setOutputURI( conf, "mongodb://localhost/test.out" );
		
		Job job = new Job(conf, "appLogSplit");
		
		
		job.setJarByClass(mainClass);
		
		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(AppLogSeparatorMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(AccessLogWritable.class);
		
		//job.setReducerClass(AppLogLastAccessReducer.class);
		job.setReducerClass(NoReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(BSONWritable.class);
		
		job.setOutputFormatClass(MongoOutputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);
		
		//FileOutputFormat.setOutputPath(job, new Path("output"));
		
		return job;
	}
}
