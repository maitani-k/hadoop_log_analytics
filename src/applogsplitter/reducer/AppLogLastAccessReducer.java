package applogsplitter.reducer;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import com.mongodb.hadoop.io.BSONWritable;

import applogsplitter.writable.AccessLogWritable;


public class AppLogLastAccessReducer extends Reducer<IntWritable, AccessLogWritable, IntWritable, BSONWritable> {

	@Override
	protected void reduce(IntWritable key, Iterable<AccessLogWritable> values, Context context) throws IOException, InterruptedException {
		
		Date maxDate = null;
		AccessLogWritable maxDateLog = null;
		for(AccessLogWritable value : values){
			
			Date date = null;
			try {
				date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(value.getDate());
			} catch (ParseException e) {
				Logger logger = Logger.getLogger(AppLogLastAccessReducer.class);
				logger.fatal(e.getMessage(), e);
				break;
			}
				
			if( maxDate == null){
				maxDate = new Date(date.getTime());
				maxDateLog = value;
			}else{
				if(maxDate.getTime() < date.getTime()){
					maxDate = new Date(date.getTime());
					maxDateLog = value.clone();
				}
			}
		}
		
		BSONWritable bson =new BSONWritable();
		bson.put("viewer", Integer.parseInt(maxDateLog.getViewerId()));
		bson.put("date"  , maxDateLog.getDate());
		bson.put("page"  , maxDateLog.getPage());
		bson.put("query" , maxDateLog.getQuery());
		
		context.write(key, bson);
	}
}
