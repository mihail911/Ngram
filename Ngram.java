/* This will implement the actual map/reduce operations.
*/

import java.io.IOException;
import java.lang.String;
import java.util.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.nio.charset.*;
import java.nio.*;
import java.io.File;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce; //newer API
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.*;

import cs149.ngram.Tokenizer;

public class Ngram {

    //Define custom input format for ngram
    public static class NgramInputFormat extends FileInputFormat<Text, Text>{
        public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context, Reporter reporter) throws IOException{
            reporter.setStatus(split.toString());
            return new PageRecordReader(context, (FileSplit)split);
        }
    }

    public static class PageRecordReader extends RecordReader<Text, Text>{
        private Path file = null;
        private Configuration jc;
        private LineRecordReader lineReader;
        private Text pageTitle;
        private Text pageText;

        public PageRecordReader(TaskAttemptContext context, FileSplit split) throws IOException {
            FileSplit newSplit = (FileSplit) genericSplit;
            file = newSplit.getPath();
            jc = context.getConfiguration();
            lineReader = new LineRecordReader(jc, newSplit);
            pageTitle = new Text("");
            pageText = new Text("");
        }
//        public void initialize (InputSplit split, JobConf job){
//         Do we need initialize method??/
//        }

        public boolean nextKeyValue() throws IOException {

            bool success = lineReader.nextKeyValue();
            if (success){ //successful key,value read

            }else{
                //Done reading
            }
        }

        //@Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return new Text("");
        }

        //@Override
        public Text getCurrentValue() {
            return new Text("");
        }

    }

//Include a combiner so that we achieve desired run-time (outputs max similarity for all pages send to a given mapper)

    public static class Map extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
        private final static IntWritable one = new IntWritable(1);

        private String queryFile;
        private int ngramSize = 0;

        //Get parameters for Ngrams
        public void configure(JobConf job){
            queryFile = job.get("queryFile");
            ngramSize = Integer.parseInt(job.get("ngramSize"));
        }
	
	//Got this code off SO--might work CHECK FUNCTIONALITY OF THIS!!!
	public String readFile(String pathname) throws IOException {
    	File file = new File(pathname);
    StringBuilder fileContents = new StringBuilder((int)file.length());
    Scanner scanner = new Scanner(file);
    String lineSeparator = System.getProperty("line.separator");
    try {
        while(scanner.hasNextLine()) {
            fileContents.append(scanner.nextLine() + lineSeparator);
        }
        return fileContents.toString();
    } finally {
        scanner.close();
    }

	}

        //Creates set of all ngrams in query document --maybe want to make this generic for the pages as well??
        public HashSet<String> generateQueryNgrams(){
            HashSet<String> allNgrams = new HashSet<String>();
		String fileContents="";
		try{
             fileContents = readFile(queryFile);
		}catch(IOException e){
			//blah
		}
            Tokenizer tokenizer = new Tokenizer(fileContents);
            ArrayList<String> tempGram = new ArrayList<String>();
            while(tokenizer.hasNext()){
                if(tempGram.size() == ngramSize){
                    String ngram = "";
                    for (String str: tempGram){ //Create string from tokens in arraylist
                        ngram += str;
                    }
                    allNgrams.add(ngram);
                    tempGram.remove(0); //Remove token at beginning
                }
                tempGram.add(tokenizer.next());
            }
            return allNgrams;
        }


        //TODO: SHOULD WE BE CASE-SENSITIVE??
        public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            //Assume mapper gets (key, value) = (title page, text of page)
            int similarityScore = 0;
            HashSet<String> queryGrams = generateQueryNgrams();
            Tokenizer tokenizer = new Tokenizer(value.toString());
            ArrayList<String> tempGram = new ArrayList<String>();
            while(tokenizer.hasNext()){
                if(tempGram.size() == ngramSize){
                    String ngram = "";
                    for (String str: tempGram){ //Create string from tokens in arraylist
                        ngram += str;
                    }
                    if(queryGrams.contains(ngram)){
                        similarityScore += 1;
                    }
                    tempGram.remove(0); //Remove token at beginning
                }
                tempGram.add(tokenizer.next());
            }

            String compositeValue = key.toString() + "," + Integer.toString(similarityScore);
            output.collect(new Text("1"), new Text(compositeValue));
        }
    }

    //May have to define a custom Combiner class that emits 20 top pages from each mapper

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
           int maxSim = 0;
            String bestPage = "";

            while(values.hasNext()){
                String[] pageScore = values.next().toString().split(","); //0 index = page title, 1 index = score
                int score = Integer.parseInt(pageScore[1]);
                if (score > maxSim ) {
                    maxSim = score;
                    bestPage = pageScore[0];
                }
            }
            output.collect(new Text(bestPage), new Text(Integer.toString(maxSim)));
           
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(Ngram.class);
        conf.setJobName("ngram");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        //Set mapper output key,value types
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(NgramInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        //Set ngram parameters
        conf.set("ngramSize", args[0]);
        conf.set("queryFile", args[1]);

        FileInputFormat.setInputPaths(conf, new Path(args[2]));
        FileOutputFormat.setOutputPath(conf, new Path(args[3]));

        JobClient.runJob(conf);
        //if necessary
        //job.waitForCompletion(true)
    }
}
