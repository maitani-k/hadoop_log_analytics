package org.klab.mapreduce.applog.lastaccess.reducer;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.klab.mapreduce.applog.AccessLogWritable;

import com.mongodb.hadoop.io.BSONWritable;



public class LastAccessReducer extends Reducer<IntWritable, AccessLogWritable, IntWritable, BSONWritable> {
	
	public static final String FETCH_COUNT_KEY = "applogspliter.reducer.applog_last_access_reducer.fetch_count";
	
	public static final int FETCH_COUNT_INFINITE = -1;
	
	private static final int DEFAULT_FETCH_COUNT = 5;
	

	@Override
	protected void reduce(IntWritable key, Iterable<AccessLogWritable> values, Context context) throws IOException, InterruptedException {
		
		List<AccessLogWritable> list = new ArrayList<>(); 
		for(AccessLogWritable value : values){
			list.add(value.clone());
		}
		
		Collections.sort(list, new Comparator<AccessLogWritable>() {
			@Override
			public int compare(AccessLogWritable o1, AccessLogWritable o2) {
				try{
					
					Date date1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(o1.getDate());
					Date date2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(o2.getDate());
					
					return  (int)(date2.getTime() - date1.getTime());
					
				}catch(ParseException e){
					Logger.getLogger(LastAccessReducer.class).warn(e.getMessage());
					return 0;
				}				
			}
		});
		
		final int limit = context.getConfiguration().getInt(FETCH_COUNT_KEY, DEFAULT_FETCH_COUNT);
		int counter = 0;
		for(AccessLogWritable ele : list){
			
			if(counter >= limit && limit != FETCH_COUNT_INFINITE){
				break;
			}
			
			BSONWritable bson =new BSONWritable();
			bson.put("viewer", Integer.parseInt(ele.getViewerId()));
			bson.put("date"  , ele.getDate());
			bson.put("page"  , ele.getPage());
			bson.put("query" , ele.getQuery());
			
			context.write(null, bson);
			
			counter++;
		}
	}
}
