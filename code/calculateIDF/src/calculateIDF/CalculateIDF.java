package calculateIDF;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;

import calculateIDF.CalculateTestIDF.CalculateTestMapper;
import calculateIDF.CalculateTestIDF.CalculateTestReducer;
import calculateIDF.TotalIDF.TotalIDFMapper;
import calculateIDF.TotalIDF.TotalIDFReducer;

public class CalculateIDF { //calculate IDF
	public static int amount; //total number of training text(from input)
	
	//map
	public static class InvertedIndexMapper extends Mapper<Object, Text, Text, Text>{
		private List<String> patternsInclude = new ArrayList<String>(); //specific patterns considered
		private Text word = new Text(); //word
		private Text file = new Text(); //filename
		
		public void setup(Context context) throws IOException,InterruptedException{
			Configuration conf = context.getConfiguration();
			URI[] patternsURIs = Job.getInstance(conf).getCacheFiles(); //specific word text
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
					patternsInclude.add(pattern); //add words
				}
			}catch (IOException ioe){
				System.err.println("Caught exception while parsing the cached file"+StringUtils.stringifyException(ioe));
			}
		}
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			FileSplit fileSplit = (FileSplit)context.getInputSplit(); //get filename
			String fileName = fileSplit.getPath().getName();
			
			String line = new String(value.getBytes(),0,value.getLength(),"GBK"); //encoding is GBK
			String[] lines = line.split("-"); //get the information
			String theline = new String();
			for(int i=1;i<lines.length;i++){
				theline += lines[i];
			}
			System.out.println(theline);
			
			Result words = ToAnalysis.parse(theline); //split the line
			List<Term> terms = words.getTerms();
			for(int i=0;i<terms.size();i++){
				if(patternsInclude.indexOf(words.get(i).getName())!=-1){ //word is specific word
					word.set(words.get(i).getName());
					file.set(fileName);
					context.write(word, file);//write the <word,filename>
				}
			}
		}
	}
	
	//reduce
	public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text>{
		private Text word = new Text(); //word
		private Text result = new Text(); //number of exist + amount
		
		public void setup(Context context) throws IOException,InterruptedException{
			Configuration conf = context.getConfiguration();
		    amount = Integer.parseInt(conf.get("amount")); //get amount from input
		    System.out.println(amount);
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException{
			HashSet<String> files = new HashSet<String>(); //filename(use set to avoid repetition
			int sum=0;
			for(Text val : values){
				files.add(val.toString());
			}
			sum = files.size();
			word.set(key.toString());
			result.set(sum+"#"+amount);
			context.write(word, result); //output <word,sum#amount>
		}
	}
	
	public static void main(String[] args) throws Exception { 
		Configuration conf1 = new Configuration(); //calculate training IDF(idea from inverted index)
		amount = Integer.parseInt(args[6]); //amount
		conf1.setInt("amount", amount);
		
		Job job1 = new Job(conf1,"calculate training IDF");
		job1.setJarByClass(CalculateIDF.class); 
		job1.addCacheFile(new Path(args[5]).toUri()); //special word text
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.setMapperClass(InvertedIndexMapper.class);
		job1.setReducerClass(InvertedIndexReducer.class);
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.waitForCompletion(true);
		
		Configuration conf2 = new Configuration(); //calculate test IDF(idea from inverted index)
		
		Job job2 = new Job(conf2,"calculate test IDF");
		job2.setJarByClass(CalculateTestIDF.class); 
		job2.addCacheFile(new Path(args[5]).toUri()); //special word text
		
		job2.setOutputKeyClass(Text.class); 
		job2.setOutputValueClass(Text.class); 
		job2.setMapperClass(CalculateTestMapper.class);
		job2.setReducerClass(CalculateTestReducer.class); 
		job2.setInputFormatClass(TextInputFormat.class); 
		job2.setOutputFormatClass(TextOutputFormat.class); 
		
		FileInputFormat.addInputPath(job2, new Path(args[2]));
		FileOutputFormat.setOutputPath(job2, new Path(args[3]));
		job2.waitForCompletion(true);
		
		Configuration conf3 = new Configuration(); //calculate IDF(idea from word count)
		
		Job job3 = new Job(conf3,"calculate IDF");
		job3.setJarByClass(TotalIDF.class); 
		
		job3.setOutputKeyClass(Text.class); 
		job3.setOutputValueClass(IntWritable.class); 
		job3.setMapperClass(TotalIDFMapper.class);
		job3.setReducerClass(TotalIDFReducer.class); 
		job3.setInputFormatClass(TextInputFormat.class); 
		job3.setOutputFormatClass(TextOutputFormat.class); 
		
		FileInputFormat.setInputPaths(job3, new Path(args[1]), new Path(args[3]));
		FileOutputFormat.setOutputPath(job3, new Path(args[4]));
		job3.waitForCompletion(true);
	}
}