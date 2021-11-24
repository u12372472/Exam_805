package Suscription_by_Month;


import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class SuscriptionDriver
{
public static void main( String[] args ) throws IOException
{
Configuration conf =new Configuration();
// Create a new Job
Job job = Job.getInstance(conf, "Count of Suscription by Month");
job.setJarByClass(SuscriptionDriver.class);
// Specify various job-specific parameters
job.setJobName("myjob");
//set the mapper and reducer
job.setMapperClass(SuscriptionMapper.class); 
job.setReducerClass(SuscriptionReducer.class);
//set the format of mapper and reducer
job.setInputFormatClass(TextInputFormat.class);
job.setOutputFormatClass(TextOutputFormat.class);
//set the key nd value format of the output
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(IntWritable.class);
//set the output and input format
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
//Submit the job, then poll for progress until the job is complete
try { job.waitForCompletion(true);
} catch (ClassNotFoundException e) {
e.printStackTrace();
} catch (InterruptedException e) {
e.printStackTrace();
}
} }