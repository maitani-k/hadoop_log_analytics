package applogsplitter.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import applogsplitter.writable.AccessLogWritable;

import com.mongodb.hadoop.io.BSONWritable;

public class NoReducer extends Reducer<IntWritable, AccessLogWritable, IntWritable, BSONWritable> {
	
	@Override
	protected void reduce(IntWritable key, Iterable<AccessLogWritable> values,Context context) throws IOException, InterruptedException {
		
		for(AccessLogWritable value : values){
			
			BSONWritable bson =new BSONWritable();
			bson.put("viewer", Integer.parseInt(value.getViewerId()));
			bson.put("date"  , value.getDate());
			bson.put("page"  , value.getPage());
			bson.put("query" , value.getQuery());
			
			context.write(null, bson);
		}
	}
}
