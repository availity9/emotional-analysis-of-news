package processTest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ProcessTest { //process test information
	//map
	public static class ProcessTestMapper extends Mapper<Object, Text, Text, Text> {
		private Text fileName = new Text();
		private Text title = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException { 
			//get filename and title
			String index = new String();
			String theline = new String();
			
			String line = value.toString();
			System.out.println(line);
			String[] lines = line.split("\t");
			if(lines.length < 6){ //sz000848 have two abnormal lines
				theline = "";
				index = "";
				System.out.println(line);
			}
			else{
				index = lines[0]+lines[1];
				theline = "";
				for(int i=4;i<lines.length-1;i++){
					theline += lines[i];
				}
			}
			fileName.set(index);
			title.set(theline);
			context.write(fileName, title); //output <filename, title>
		} 
	}
	
	//reduce
	public static class ProcessTestReducer extends Reducer<Text, Text, Text, Text>{
		private Text titles = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException{
			String temp = new String();
			for(Text val : values){
				temp += (" "+val.toString());
			}
			titles.set(temp);
			context.write(key, titles); //output <fileName,titles>
		}
	}
	
	public static void main(String[] args) throws Exception { 
		Configuration conf = new Configuration(); //process test
		Job job = new Job(conf,"process test");
		job.setJarByClass(ProcessTest.class); 
	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(ProcessTestMapper.class);
		job.setReducerClass(ProcessTestReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
	
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
}