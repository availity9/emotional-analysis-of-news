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
import org.apache.hadoop.util.StringUtils;

public class CalculateTestTFIDF { //calculate test TF-IDF
	//map
	public static class CalculateTestTFIDFMapper extends Mapper<Object, Text, Text, DoubleWritable>{
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
			//get index and titles
			String index = new String();
			String theline = new String();
			
			String line = value.toString();
			System.out.println(line);
			String[] lines = line.split("\t");
			index = lines[0];
			for(int i=1;i<lines.length;i++){
				theline += lines[i];
			}
			
			Result words = ToAnalysis.parse(theline); //split the line
			List<Term> terms = words.getTerms();
			List<String> sterms = new ArrayList<String>(); //specific words in line
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
				int count = 1; //number of appearance of the word
				if(sterms.indexOf(theword)==i){ //first appearance is i
					for(int j=i+1;j<sterms.size();j++){
						if(sterms.get(j)==theword){
							count += 1;
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
				word.set(index+"&"+theword);
				result.set(tf*idf);
				context.write(word, result); //output <filename&word, TF-IDF>
			}
		}
	}
}