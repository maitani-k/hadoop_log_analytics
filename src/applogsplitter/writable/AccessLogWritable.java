package applogsplitter.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

public class AccessLogWritable implements Writable, Cloneable {
	
	private static final Pattern pattern = Pattern.compile("^(w[0-9]{3,4}) ([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}) .+? \"(.+?) (.+?)\" .+? \".+?\" \"(.+?)\"(.*)$");
	
	private static final Pattern pattern2 = Pattern.compile("opensocial_viewer_id=([0-9]+)");
	
	private String host;
	private String date;
	private String method;
	private String path;
	private String page;
	private String query;
	private String ua;
	private String other;
	private String viewerId;
	
	public AccessLogWritable(){}
	
	private AccessLogWritable(String host, String date, String method, String path, String ua, String other){
		this.host = host;
		this.date = date;
		this.method = method;
		this.path = path;
		this.ua = ua;
		this.other = other;
		
		Matcher matcher = pattern2.matcher(this.path);
		if(matcher.find()){;
			this.viewerId = matcher.group(1);
		}else{
			this.viewerId = "0";
			Logger.getLogger(AccessLogWritable.class).info("viewer id couldn't get.");
		}
		
		String[] splited = StringUtils.split(this.path, "?", 2);
		if(splited.length ==2 ){
			this.page  = splited[0];
			this.query = splited[1];
		}else{
			this.page  = splited[0];
			this.query = "";
		}
	}
	
	public static AccessLogWritable create(String line){
		Matcher matcher = pattern.matcher(line);
		if(matcher.find()){
			return new AccessLogWritable(matcher.group(1), matcher.group(2), matcher.group(3), matcher.group(4), matcher.group(5), matcher.group(6));
		}else{
			throw new IllegalArgumentException("feeded line couldn't compile. [" + line + "]");
		}
	}

	public static Pattern getPattern() {
		return pattern;
	}

	public String getHost() {
		return host;
	}

	public String getDate() {
		return date;
	}

	public String getMethod() {
		return method;
	}

	public String getPath() {
		return path;
	}

	public String getPage() {
		return page;
	}

	public String getQuery() {
		return query;
	}

	public String getUa() {
		return ua;
	}
	
	public String getOther() {
		return other;
	}

	public String getViewerId() {
		return viewerId;
	}
	
	public IntWritable getViewerIdByIntWritable(){
		return new IntWritable(Integer.parseInt(this.viewerId));
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.host     = Text.readString(in);
		this.viewerId = Text.readString(in);
		this.date     = Text.readString(in);
		this.method   = Text.readString(in);
		this.path     = Text.readString(in);
		this.page     = Text.readString(in);
		this.query    = Text.readString(in);
		this.ua       = Text.readString(in);
		this.other    = Text.readString(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, this.host);
		Text.writeString(out, this.viewerId);
		Text.writeString(out, this.date);
		Text.writeString(out, this.method);
		Text.writeString(out, this.path);
		Text.writeString(out, this.page);
		Text.writeString(out, this.query);
		Text.writeString(out, this.ua);
		Text.writeString(out, this.other);
	}

	@Override
	public String toString(){
		return String.format(
			"%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s",
			viewerId, host, date, method, path, page, query, ua, other
		);
	}
	
	@Override
	public AccessLogWritable clone(){
		return new AccessLogWritable(
			this.host,
			this.date,
			this.method,
			this.path,
			this.ua,
			this.other
		);
	}
}
