package Suscription_by_Month;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists; 

public class SuscriptionMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{ @Override 
	public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException { IntWritable one = new IntWritable(1);
String line = value.toString();
List<String> items = Lists.newArrayList(Splitter.on(',').split(line));
String Suscriptions = items.get(22);
String Month=items.get(10); Text compositeKey = new Text(Suscriptions+ " " + Month);
context.write(compositeKey,one); } }