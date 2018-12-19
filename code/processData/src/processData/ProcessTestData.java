package processData;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

public class ProcessTestData { //vectored test data
	public static int amount = 0; //number of index of vector
	
	public static class ProcessTestDataMapper extends Mapper<Object, Text, Text, Text>{
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
			
			String u = new String(patternsInclude.indexOf(theword)+"#"+value.toString().split("\t")[1]); //index of vector # TF-IDF
			info.set(u);
			context.write(title, info);
		}
	}
	
	//reduce
	public static class ProcessTestDataReducer extends Reducer<Text, Text, Text, Text>{
		private Text attr = new Text(); //instance output
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException{
			double kind = -1.0; //initial kind
			
			ArrayList<Double> train = new ArrayList<Double>(amount); //vector
			for(int i=0;i<amount;i++){ //initial
				train.add(0.0);
			}
			for(Text val : values){
				int index = Integer.parseInt(val.toString().split("#")[0]);
				double tfidf = Double.parseDouble(val.toString().split("#")[1]);
				train.set(index, tfidf);
			}
			
			String line = new String(); //output
			line += train.get(0);
			for(int i=1;i<train.size();i++){
				line += (","+train.get(i));
			}
			attr.set(line+","+kind);
			System.out.println(attr);
			context.write(key, attr); //output <filename,instance>
		}
	}
}