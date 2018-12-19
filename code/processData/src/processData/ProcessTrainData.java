package processData;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;

import processData.ProcessTestData.ProcessTestDataMapper;
import processData.ProcessTestData.ProcessTestDataReducer;

public class ProcessTrainData { //vectored train data
	public static int amount = 0; //number of vector index
	
	//map
	public static class ProcessTrainDataMapper extends Mapper<Object, Text, Text, Text>{
		private List<String> patternsInclude = new ArrayList<String>(); //specific patterns considered(from totalIDF)
		private Text title = new Text();
		private Text info = new Text();
		
		public void setup(Context context) throws IOException,InterruptedException{
			Configuration conf = context.getConfiguration();
			URI[] patternsURIs = Job.getInstance(conf).getCacheFiles(); //specific word text(totalIDF)
			for(URI patternsURI : patternsURIs){
				Path patternsPath = new Path(patternsURI.getPath());
				String patternsFileName = patternsPath.getName().toString();
				parseSkipFile(patternsFileName);
			}
		}
		
		private void parseSkipFile(String fileName){
			try{
				BufferedReader fis = new BufferedReader(new FileReader(fileName));
				String pattern = null;
				while((pattern = fis.readLine())!=null){
					patternsInclude.add(pattern.split("\t")[0]); //add words
				}
			}catch (IOException ioe){
				System.err.println("Caught exception while parsing the cached file"+StringUtils.stringifyException(ioe));
			}
		}
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			amount = patternsInclude.size();
			
			System.out.println(value);
			String temp = value.toString().split("\t")[0];
			String theword = temp.split("&")[1]; //word
			String thetitle = temp.split("&")[0]; //title
			title.set(thetitle);
			
			String u = new String(patternsInclude.indexOf(theword)+"#"+value.toString().split("\t")[1]); //index in vector # TF-IDF
			info.set(u);
			context.write(title, info);
		}
	}
	
	//reduce
	public static class ProcessTrainDataReducer extends Reducer<Text, Text, NullWritable, Text>{
		private Text attr = new Text(); //instance output
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException{
			//numbered label
			String temp = key.toString().split("#")[1];
			double kind = 0;
			if(temp.equals("negative.txt"))	kind = 1.0;
			else if(temp.equals("neutral.txt"))	kind = 2.0;
			else if(temp.equals("positive.txt"))	kind = 3.0;
			else{
				System.out.println(temp+"doesn't have this kind!");
			}
			
			ArrayList<Double> train = new ArrayList<Double>(amount); //vector
			for(int i=0;i<amount;i++){ //initial
				train.add(0.0);
			}
			for(Text val : values){
				int index = Integer.parseInt(val.toString().split("#")[0]);
				double tfidf = Double.parseDouble(val.toString().split("#")[1]);
				train.set(index, tfidf);
			}
			
			String line = new String(); //instance output
			line += train.get(0);
			for(int i=1;i<train.size();i++){
				line += (","+train.get(i));
			}
			attr.set(line+","+kind);
			System.out.println(attr);
			context.write(NullWritable.get(), attr); //output <instance>
		}
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		Configuration conf1 = new Configuration(); //vectored train data
		
		Job job1 = new Job(conf1,"process train data");
		job1.setJarByClass(ProcessTrainData.class); 
		job1.addCacheFile(new Path(args[4]).toUri()); //total IDF
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.setMapperClass(ProcessTrainDataMapper.class);
		job1.setReducerClass(ProcessTrainDataReducer.class);
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.waitForCompletion(true);
		
		Configuration conf2 = new Configuration(); //vectored test data
		
		Job job2 = new Job(conf2,"process test data");
		job2.setJarByClass(ProcessTestData.class); 
		job2.addCacheFile(new Path(args[4]).toUri()); //total IDF
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setMapperClass(ProcessTestDataMapper.class);
		job2.setReducerClass(ProcessTestDataReducer.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job2, new Path(args[2]));
		FileOutputFormat.setOutputPath(job2, new Path(args[3]));
		job2.waitForCompletion(true);
	}
}