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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

public class CalculateTestIDF { //calculate test IDF
	public static int amount = 0; //total number of text
	
	//map
	public static class CalculateTestMapper extends Mapper<Object, Text, Text, Text> {
		private Text word = new Text();
		private Text file = new Text();
		private List<String> patternsInclude = new ArrayList<String>(); //specific words
		
		public void setup(Context context) throws IOException,InterruptedException{
			Configuration conf = context.getConfiguration();
			URI[] patternsURIs = Job.getInstance(conf).getCacheFiles(); //specific words text
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
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException { 
			//get index and titles
			String index = new String();
			String theline = new String();
			
			String line = value.toString();
			System.out.println(line);
			String[] lines = line.split("\t");
			index = lines[0];
			for(int i=1;i<lines.length;i++){
				theline +=lines[i]; 
			}
			amount += 1;
			
			Result words = ToAnalysis.parse(theline); //split the line
			List<Term> terms = words.getTerms();
			for(int i=0;i<terms.size();i++){
				if(patternsInclude.indexOf(words.get(i).getName())!=-1){ //word is specific word
					word.set(words.get(i).getName());
					file.set(index);
					context.write(word, file);//write the <word,filename>
				}
			}
		} 
	}
	
	//reduce
	public static class CalculateTestReducer extends Reducer<Text, Text, Text, Text>{
		private Text word = new Text();
		private Text result = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException{
			HashSet<String> files = new HashSet<String>(); //use set to avoid repetition
			int sum=0;
			for(Text val : values){
				files.add(val.toString());
			}
			sum = files.size();
			word.set(key.toString());
			result.set(sum+"#"+amount);
			context.write(word, result); //output <word,sum	amount>
		}
	}
}