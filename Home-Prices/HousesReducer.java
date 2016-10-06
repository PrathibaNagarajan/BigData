package Houses;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import java.io.IOException;
import java.text.DecimalFormat;


public class HousesReducer  extends Reducer <Text,IntWritable,Text,FloatWritable> {

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		// TODO: initialize min and max values
		Long min = Long.MAX_VALUE;
		Long max = Long.MIN_VALUE;
		int numberOfSales = 0;
		int salePrice = 0;
		Float sum = 0.0f;
		// TODO: loop through values to determine min, max, count, and sum 
		for(IntWritable price:values){
			// TODO: calculate mean
			salePrice = price.get();
			numberOfSales++;
			sum += salePrice;
			min = (salePrice < min) ? salePrice : min;
			max = (salePrice > max) ? salePrice : max;
		}
		Float mean = sum/numberOfSales;
		// TODO: write (key, min) to context
		context.write(key,new FloatWritable(min));
		// TODO: write (key, mean) to context
		context.write(key,new FloatWritable(mean));
		// TODO: write (key, max) to context
		context.write(key,new FloatWritable(max));
   	}
}
