package applogsplitter.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import applogsplitter.writable.AccessLogWritable;



public class AppLogSeparatorMapper extends Mapper<LongWritable, Text, IntWritable, AccessLogWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		try {
			AccessLogWritable log = AccessLogWritable.create(value.toString());
			
			int viewerId =Integer.parseInt(log.getViewerId());
			if(viewerId != 0){
				context.write(new IntWritable(viewerId), log);
			}
			
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}
}
