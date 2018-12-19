package naiveBayes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

public class NaiveBayes { //naive Bayes
	//map
	public static class NaiveBayesMapper extends Mapper<Object, Text, Text, Text>{
		private ArrayList<Instance> trainSet; //train set
		private Text word = new Text();
		private Text result = new Text();
		
		@Override
		protected void setup(Context context) throws IOException,InterruptedException{
			trainSet = new ArrayList<Instance>(); //initial
			
			Configuration conf = context.getConfiguration();
			URI[] patternsURIs = Job.getInstance(conf).getCacheFiles(); //kind vector
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
					Instance trainInstance = new Instance(pattern.split("\t")[1]); //add instance for one kind
					trainSet.add(trainInstance);
				}
			}catch (IOException ioe){
				System.err.println("Caught exception while parsing the cached file"+StringUtils.stringifyException(ioe));
			}
		}
		
		public void map(Object key, Text textLine, Context context) throws IOException, InterruptedException {
			ArrayList<Double> scores = new ArrayList<Double>(3); //score
			for(int i=0;i<3;i++)	scores.add(0.0); //initial
			
			System.out.println(textLine);
			String temp = new String();
			temp = textLine.toString().split("\t")[0]; //title
			Instance testInstance = new Instance(textLine.toString().split("\t")[1]);
			for(int i = 0;i < testInstance.getAtrributeValue().length;i++){
				if(testInstance.getAtrributeValue()[i]!=0.0){ //have this word
					for(Instance train : trainSet){ //use naive formula
						double t = scores.get((int)train.getLable()-1)+Math.log(train.getAtrributeValue()[i]+1);
						scores.set((int)train.getLable()-1, t);
					}
				}
			}
			System.out.println(scores);
			
			//find max score and set the kind
			int max = 1; //initial neutral
			if(scores.get(0)>scores.get(max))	max=0;
			if(scores.get(2)>scores.get(max))	max=2;
			
			String kind = new String();
			if(max == 0)	kind = "negative";
			else if(max ==1)	kind = "neutral";
			else	kind = "positive";
			word.set(temp);
			result.set(kind);
		    context.write(word, result);
		}
	}
}