package calculateTFIDF;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;

import calculateTFIDF.CalculateTestTFIDF.CalculateTestTFIDFMapper;

public class CalculateTFIDF { //calculate training TF-IDF
	//map
	public static class CalculateTrainTFIDFMapper extends Mapper<Object, Text, Text, DoubleWritable>{
		private Map<String,Double> IDF = new HashMap<String, Double>(); //IDF map
		private Text word = new Text(); //word
		private DoubleWritable result = new DoubleWritable(); //TF-IDF
		
		public void setup(Context context) throws IOException,InterruptedException{
			Configuration conf = context.getConfiguration();
			URI[] patternsURIs = Job.getInstance(conf).getCacheFiles(); //IDF
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
					IDF.put(pattern.split("\t")[0], Double.parseDouble(pattern.split("\t")[1])); //add (word:IDF)
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
			List<String> sterms = new ArrayList<String>(); //all speicific word
			for(int i=0;i<terms.size();i++){
				String theword = words.get(i).getName();
				if(IDF.containsKey(theword)){ //word is specific word
					sterms.add(theword);
				}
			}
			System.out.println(sterms);
			int all = sterms.size();
			
			for(int i=0;i<sterms.size();i++){
				String theword = sterms.get(i);
				int count = 1;
				if(sterms.indexOf(theword)==i){ //the first appearance is i
					for(int j=i+1;j<sterms.size();j++){
						if(sterms.get(j)==theword){
							count += 1; //number of appearance of the word
						}
					}
				}
				else{
					continue;
				}
				
				//calculate TF-IDF
				double tf = 0;
				if(all!=0){
					tf = (double)count/(double)all;
				}
				double idf = IDF.get(theword);
				word.set(fileName+"&"+theword);
				result.set(tf*idf);
				context.write(word, result); //output <filename&word, TF-IDF>
			}
		}
	}
	
	public static void main(String[] args) throws Exception { 
		Configuration conf1 = new Configuration(); //calculate training TFIDF
		
		Job job1 = new Job(conf1,"calculate training tf-idf");
		job1.setJarByClass(CalculateTFIDF.class);
		job1.addCacheFile(new Path(args[4]).toUri()); //total IDF
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(DoubleWritable.class);
		job1.setMapperClass(CalculateTrainTFIDFMapper.class);
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.waitForCompletion(true);
		
		Configuration conf2 = new Configuration(); //calculate test TFIDF
		
		Job job2 = new Job(conf2,"calculate test tf-idf");
		job2.setJarByClass(CalculateTFIDF.class);
		job2.addCacheFile(new Path(args[4]).toUri()); //total IDF
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(DoubleWritable.class);
		job2.setMapperClass(CalculateTestTFIDFMapper.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job2, new Path(args[2]));
		FileOutputFormat.setOutputPath(job2, new Path(args[3]));
		job2.waitForCompletion(true);
	}
}