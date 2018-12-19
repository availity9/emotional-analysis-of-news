package classKNN;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;

public class ClassKNN { //KNN
	//map
	public static class KNNMap extends Mapper<Object, Text, Text, ListWritable<DoubleWritable>>{
		private int k; //number of class
		private ArrayList<Instance> trainSet; //train set
		private Text word = new Text();
		
		@Override
		protected void setup(Context context) throws IOException,InterruptedException{
			k = context.getConfiguration().getInt("k", 1); //initial
			trainSet = new ArrayList<Instance>();
			
			Configuration conf = context.getConfiguration();
			URI[] patternsURIs = Job.getInstance(conf).getCacheFiles(); //training set
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
					Instance trainInstance = new Instance(pattern); //add train instance
					trainSet.add(trainInstance);
				}
			}catch (IOException ioe){
				System.err.println("Caught exception while parsing the cached file"+StringUtils.stringifyException(ioe));
			}
		}
		
		//calculate distance
		private double EuclideanDistance(double[] a,double[] b) throws Exception{
			if(a.length != b.length)
				System.out.println("size not compatible!");
			double sum = 0.0;
	        for(int i = 0;i < a.length;i++){
				sum += Math.pow(a[i] - b[i], 2);
	        }
			return Math.sqrt(sum);
		}
		
		public void map(Object key, Text textLine, Context context) throws IOException, InterruptedException {
			ArrayList<Double> distance = new ArrayList<Double>(k); //all the current nearest distance value
			ArrayList<DoubleWritable> trainLable = new ArrayList<DoubleWritable>(k); //corresponding label
			String temp = new String();
			
			for(int i = 0;i < k;i++){ //initial
				distance.add(Double.MAX_VALUE);
				trainLable.add(new DoubleWritable(-1.0));
			}
			ListWritable<DoubleWritable> lables = new ListWritable<DoubleWritable>(DoubleWritable.class);		
			
			System.out.println(textLine);
			temp = textLine.toString().split("\t")[0]; //title
			Instance testInstance = new Instance(textLine.toString().split("\t")[1]); //vector
			for(int i = 0;i < trainSet.size();i++){
				try {
					double dis = EuclideanDistance(trainSet.get(i).getAtrributeValue(), testInstance.getAtrributeValue());
					int index = indexOfMax(distance);
					if(dis < distance.get(index)){ //replace the max distance
						distance.remove(index);
					    trainLable.remove(index);
					    distance.add(dis);
					    trainLable.add(new DoubleWritable(trainSet.get(i).getLable()));
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}			
			word.set(temp);
			lables.setList(trainLable);
		    context.write(word, lables);
		}
		
		//find max's index of array
		public int indexOfMax(ArrayList<Double> array){
			int index = -1;
			Double min = Double.MIN_VALUE; 
			for (int i = 0;i < array.size();i++){
				if(array.get(i) > min){
					min = array.get(i);
					index = i;
				}
			}
			return index;
		}
	}
	
	//reduce
	public static class KNNReduce extends Reducer<Text,ListWritable<DoubleWritable>,Text,Text>{
		private Text kind = new Text(); //label
		
		@Override
		public void reduce(Text key, Iterable<ListWritable<DoubleWritable>> kLables, Context context) throws IOException, InterruptedException{
			DoubleWritable predictedLable = new DoubleWritable();
			for(ListWritable<DoubleWritable> val: kLables){
				try {
					predictedLable = valueOfMostFrequent(val); //most frequent
					break;
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			//transfer double to label
			String lab = new String();
			double pred = predictedLable.get();
			if(pred == 1.0)	lab = "negative";
			else if(pred == 2.0)	lab = "neutral";
			else	lab = "positive";
			kind.set(lab);
			context.write(key, kind);
		}
		
		//most frequent
		public DoubleWritable valueOfMostFrequent(ListWritable<DoubleWritable> list) throws Exception{
			if(list.isEmpty())
				throw new Exception("list is empty!");
			else{
				HashMap<DoubleWritable,Integer> tmp = new HashMap<DoubleWritable,Integer>(); //(label:count)
				for(int i = 0 ;i < list.size();i++){
					if(tmp.containsKey(list.get(i))){
						Integer frequence = tmp.get(list.get(i)) + 1;
						tmp.remove(list.get(i));
						tmp.put(list.get(i), frequence);
					}else{
						tmp.put(list.get(i), new Integer(1));
					}
				}

				DoubleWritable value = new DoubleWritable();
				Integer frequence = new Integer(Integer.MIN_VALUE);
				Iterator<Entry<DoubleWritable, Integer>> iter = tmp.entrySet().iterator();
				while (iter.hasNext()) {
				    Map.Entry<DoubleWritable,Integer> entry = (Map.Entry<DoubleWritable,Integer>) iter.next();
				    if(entry.getValue() > frequence){
				    	frequence = entry.getValue();
				    	value = entry.getKey();
				    }
				}
				return value;
			}
		}
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		Job kNNJob = new Job();
		kNNJob.setJobName("kNNJob");
		kNNJob.setJarByClass(ClassKNN.class);
		kNNJob.addCacheFile(new Path(args[2]).toUri()); //train set
		kNNJob.getConfiguration().setInt("k", Integer.parseInt(args[3])); //class number
		
		kNNJob.setMapperClass(KNNMap.class);
		kNNJob.setMapOutputKeyClass(Text.class);
		kNNJob.setMapOutputValueClass(ListWritable.class);

		kNNJob.setReducerClass(KNNReduce.class);
		kNNJob.setOutputKeyClass(Text.class);
		kNNJob.setOutputValueClass(Text.class);

		kNNJob.setInputFormatClass(TextInputFormat.class);
		kNNJob.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(kNNJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(kNNJob, new Path(args[1]));
		
		kNNJob.waitForCompletion(true);
	}
}