package org.klab.mapreduce.applog.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.klab.mapreduce.JobExtended;
import org.klab.mapreduce.applog.AccessLogWritable;
import org.klab.mapreduce.applog.lastaccess.reducer.LastAccessReducer;
import org.klab.mapreduce.applog.separator.mapper.SeparatorMapper;


import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoURI;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;

public class SamplingLastAccessDriver extends Configured implements Tool {
	
	private static final int DEFAULT_SAMPLING_COUNT = 3;
	
	private static final String OUTPUT_MONGODB_URI = "mongodb://localhost/";
	
	public void printUsage(){
		System.out.println(
			"args : <input> <output_collection_name> [sampling_count]" + System.lineSeparator() +
			"\tinput                  : input directory or file path where applog exists" + System.lineSeparator() +
			"\toutput_collection_name : specify output db and collection name of mongoDB" + System.lineSeparator() +
			"\t                         {db_name}.{collection_name}" + System.lineSeparator() +
			"\tsampling_count[Option] : sampling count of last access per viewer(default : 3)"
		);
	}

	@Override
	public int run(String[] args) throws Exception {
		
		if(args.length < 2){
			printUsage();
			return -1;
		}
		
		String inputDir         = args[0];
		String outputCollection = args[1];
		int samplingCount = SamplingLastAccessDriver.DEFAULT_SAMPLING_COUNT;
		if(args.length == 3){
			samplingCount = Integer.parseInt(args[2]);
		}
		
		JobExtended job = new JobExtended(
			this.getConf(), "applog_sampling_last_access", SamplingLastAccessDriver.class
		);
		
		
		//setting input from file
		FileInputFormat.setInputPaths(job, inputDir);
		job.setInputFormatClass(TextInputFormat.class);
		
		//setting output to mongodb
		String mongoDbUri = SamplingLastAccessDriver.OUTPUT_MONGODB_URI;
		MongoURI uri = new MongoURI(mongoDbUri + outputCollection);
		MongoConfigUtil.setOutputURI(job.getConfiguration(), uri.toString());
		
		//create indexes on output collection
		Mongo mongo = new Mongo(uri);
		DBCollection collection = mongo.getDB(uri.getDatabase()).getCollection(uri.getCollection());
		BasicDBObject dbo = new BasicDBObject();
		dbo.put("viewer", 1);
		dbo.put("page", 1);
		dbo.put("date", -1);
		collection.ensureIndex(dbo);
		
		job.setOutputFormatClass(MongoOutputFormat.class);
		
		
		//setting mapper
		job.setMapperClass(SeparatorMapper.class, IntWritable.class, AccessLogWritable.class);
		
		//setting reducer
		job.setReducerClass(LastAccessReducer.class, IntWritable.class, BSONWritable.class);
		job.getConfiguration().setInt(LastAccessReducer.FETCH_COUNT_KEY, samplingCount);
		
		
		//execute
		if( job.waitForCompletion(false) ){
			return 0;
		}else{
			return -1;
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new SamplingLastAccessDriver(), args));
	}
}
