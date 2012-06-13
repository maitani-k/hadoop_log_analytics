package applogsplitter;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.bson.BSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoURI;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;

import applogsplitter.mapper.AppLogSeparatorMapper;
import applogsplitter.reducer.AppLogLastAccessReducer;
import applogsplitter.reducer.NoReducer;
import applogsplitter.writable.AccessLogWritable;

public class JobFactory {

	public static Job getApplogSplitJob(Configuration conf, Class<?> mainClass) throws IOException{
		
		MongoURI uri = new MongoURI("mongodb://localhost/analytics.applog");
		
		MongoConfigUtil.setOutputURI( conf, uri.toString() );
		conf.setInt(AppLogLastAccessReducer.FETCH_COUNT_KEY, 3);
		
		Mongo mongo = new Mongo(uri);
		DBCollection collection = mongo.getDB(uri.getDatabase()).getCollection(uri.getCollection());
		BasicDBObject dbo = new BasicDBObject();
		dbo.put("viewer", 1);
		dbo.put("page", 1);
		dbo.put("date", -1);
		collection.ensureIndex(dbo);
		
		Job job = new Job(conf, "appLogSplit");
		
		
		job.setJarByClass(mainClass);
		
		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(AppLogSeparatorMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(AccessLogWritable.class);
		
		job.setReducerClass(AppLogLastAccessReducer.class);
		//job.setReducerClass(NoReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(BSONWritable.class);
		
		job.setOutputFormatClass(MongoOutputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);
		
		//FileOutputFormat.setOutputPath(job, new Path("output"));
		
		return job;
	}
}
