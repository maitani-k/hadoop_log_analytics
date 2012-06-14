package org.klab.mapreduce;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * this class extends Job, for syntax sugar.<br/>
 * this class be able to setting job more simple.<br/>
 * 
 * @author maitani-k
 */
public class JobExtended extends Job {

	public JobExtended() throws IOException {
		super();
	}
	
	public JobExtended(Configuration conf, String jobName, Class<?> cls) throws IOException {
		super(conf, jobName);
		this.setJarByClass(cls);
	}

	
	public void setMapperClass(
		Class<? extends Mapper<?,?,?,?>> cls, Class<?> outputKeyClass, Class<?> outputValueClass
	) throws IllegalStateException {
		
		super.setMapperClass(cls);
		super.setMapOutputKeyClass(outputKeyClass);
		super.setMapOutputValueClass(outputValueClass);
	}

	
	public void setReducerClass(
		Class<? extends Reducer<?,?,?,?>> cls, Class<?> outputKeyClass, Class<?> outputValueClass
	) throws IllegalStateException {
		
		super.setReducerClass(cls);
		super.setOutputKeyClass(outputKeyClass);
		super.setOutputValueClass(outputValueClass);
	}
}
