package Houses;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.util.regex.Pattern;


import java.io.IOException;
import java.util.StringTokenizer;

public class HousesMapper  extends Mapper <LongWritable,Text,Text,IntWritable> {
	private static Log log = LogFactory.getLog(HousesMapper.class);

   	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// TODO: skip  very first record (schema line)
		if(key.get() == 0)
			return;
      		// TODO: create iterator over record assuming comma-separated fields
		String[] fields = value.toString().split(",");
      		// TODO validate number of tokens in iterator 
		// TODO if invalid, then write a message to log
		if(fields.length < 81){
			log.debug("Invalid number of fields");
			return;
		}
		// TODO get neighborhodd
		// TODO validate string is not empty or null
		// TODO if empty or null, then write a message to log 
		String place = fields[12];
		if(place == null || place.isEmpty()){
			log.debug("Empty/Null neighbourhood name");
			return;
		}
		// TODO get price
		// TODO convert price to int
		Integer price = Integer.parseInt(fields[80]);
      		// TODO validate the price is greater than zero 
		// TODO if price <= 0, then write a message to log
		if(price <= 0){
			log.debug("Invalid price");
			return;
		}
		else{
      			// TODO emit key-value as (neighborhood, price) 
      			context.write(new Text(place), new IntWritable(price));
		}
   	}

}
