package calculateIDF;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class TotalIDF { //calculate whole IDF(training + test)
	//map
	public static class TotalIDFMapper extends Mapper<Object, Text, Text, IntWritable> { 
		private IntWritable count = new IntWritable(); //count of words in one text
		private Text word = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException { 
			String line = value.toString();
			System.out.println(line);
			String[] lines = line.split("\t");
			word.set(lines[0]);
			count.set(Integer.parseInt(lines[1].split("#")[0]));
			context.write(word, count);
		} 
	}
	
	//reduce
	public static class TotalIDFReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> { 
		private DoubleWritable result = new DoubleWritable(); //IDF
		private int total;
		
		public void setup(Context context) throws IOException,InterruptedException{
			total = 4767; //total text number(can modify)
		}
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException { 
			int sum = 0; 
			double idf = 0;
			for (IntWritable val : values) { 
				sum += val.get(); 
			}
			idf = Math.log(((double)total+1)/((double)sum+1))+1;
			result.set(idf);
			context.write(key, result); //output <word,IDF>
		} 
	}
}