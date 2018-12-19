package naiveBayes;

import java.io.IOException;
import java.util.ArrayList;

import naiveBayes.NaiveBayes.NaiveBayesMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ClassTFIDF { //calculate the vector for three kind
	public static int len = 0; //vector length
	
	//map
	public static class ClassTFIDFMapper extends Mapper<Object, Text, DoubleWritable, ListWritable<DoubleWritable>>{
		private DoubleWritable type = new DoubleWritable(); //label
		ListWritable<DoubleWritable> att = new ListWritable<DoubleWritable>(DoubleWritable.class); //vector
		
		public void map(Object key, Text textLine, Context context) throws IOException, InterruptedException {
			System.out.println(textLine);
			Instance sample = new Instance(textLine.toString()); //instance
			len = sample.getAtrributeValue().length;
			
			ArrayList<DoubleWritable> train = new ArrayList<DoubleWritable>(len); //convert instance to ListWritable
			for(int i=0;i<len;i++){
				DoubleWritable temp = new DoubleWritable();
				temp.set(sample.getAtrributeValue()[i]);
				train.add(temp);
			}
			att.setList(train);
			
			double label = sample.getLable();
			type.set(label);
			context.write(type, att);
		}
	}
	
	//reduce
	public static class ClassTFIDFReducer extends Reducer<DoubleWritable,ListWritable<DoubleWritable>,IntWritable,Text>{
		private Text result = new Text();
		private IntWritable count = new IntWritable(); //number of vector in this kind
		
		@Override
		public void reduce(DoubleWritable key, Iterable<ListWritable<DoubleWritable>> kLables, Context context) throws IOException, InterruptedException{
			ArrayList<Double> temp = new ArrayList<Double>(len);
			for(int i=0;i<len;i++)	temp.add(0.0); //initial
			int amount = 0; //count
			for(ListWritable<DoubleWritable> val: kLables){
				amount += 1;
				for(int i=0;i<len;i++){
					double sum =temp.get(i)+val.get(i).get();
					temp.set(i, sum);
				}
			}
			count.set(amount);
			
			//unity the vector to correspond to naive
			double all=0;
			for(int i=0;i<len;i++)	all += temp.get(i);
			for(int i=0;i<len;i++){
				double old = temp.get(i);
				temp.set(i, old/all);
			}
			
			String sresult = temp.get(0).toString();
			for(int i=1;i<len;i++){
				sresult += ","+temp.get(i).toString();
			}
			sresult += (","+key.toString()); //add label
			result.set(sresult);
			
			context.write(count, result); //output <number, instance>
		}
		
		public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
			Configuration conf1 = new Configuration(); //class TF-IDF
			
			Job job1 = new Job(conf1,"class TF-IDF");
			job1.setJarByClass(ClassTFIDF.class); 
			
			job1.setOutputKeyClass(DoubleWritable.class);
			job1.setOutputValueClass(ListWritable.class);
			job1.setMapperClass(ClassTFIDFMapper.class);
			job1.setReducerClass(ClassTFIDFReducer.class);
			job1.setInputFormatClass(TextInputFormat.class);
			job1.setOutputFormatClass(TextOutputFormat.class);
			
			FileInputFormat.addInputPath(job1, new Path(args[0]));
			FileOutputFormat.setOutputPath(job1, new Path(args[1]));
			job1.waitForCompletion(true);
			
			Configuration conf2 = new Configuration(); //naive Bayes
			
			Job job2 = new Job(conf2,"naive bayes");
			job2.setJarByClass(NaiveBayes.class); 
			job2.addCacheFile(new Path(args[4]).toUri()); //vector for kind
			
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			job2.setMapperClass(NaiveBayesMapper.class);
			job2.setInputFormatClass(TextInputFormat.class);
			job2.setOutputFormatClass(TextOutputFormat.class);
			
			FileInputFormat.addInputPath(job2, new Path(args[2]));
			FileOutputFormat.setOutputPath(job2, new Path(args[3]));
			job2.waitForCompletion(true);
		}
	}
}