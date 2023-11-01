package avgTemp;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AvgTemperatureMapper
extends Mapper<LongWritable, Text, Text, IntWritable> {

private static final int MISSING = 9999;

/*
 * 
 * 
Now we have the the weather data set (same format as Lecture 7 page 3).
What are the average air temperatures at 11pm each month? Please disregard the lines with missing temperature values and invalid quality codes. In the output, each line should be the year and month (e.g. 195001), followed by the average. 
Use the given code to implement your map and reduce functions. 

In A1, the provided data sets do not have temperature recorded for 11pm. There are two options here:

1. If you have already completed Assignment 1, and it currently uses 11pm for temperature data, you can keep it as it is.

2. If you are still working on Assignment 1, you have the option to generate the average temperature for 1pm instead of using 11pm.
 */



@Override
public void map(LongWritable key, Text value, Context context)
   throws IOException, InterruptedException {
   // Your mapper implementation here
    String line = value.toString();
    int airTemperature;

    
    String year = line.substring(15,19);
    String month = line.substring(19,21);
    String time = line.substring(23,27);
    String quality = line.substring(92,93);
    
	//time check
    if (time.equals("1300")){
    	if (line.charAt(87) == '+') {
    		airTemperature = Integer.parseInt(line.substring(88,92));
    				
    	} else {
    		airTemperature = Integer.parseInt(line.substring(87,92));
    	}
    	if (airTemperature != MISSING && quality.matches ("[01459]")) {
    		context.write(new Text(year + month), new IntWritable (airTemperature));
    	}
    
 
    }

}
}

