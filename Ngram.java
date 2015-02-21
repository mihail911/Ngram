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
import java.util.Arrays;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
//import org.apache.hadoop.mapreduce.*; //newer API
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.*;

import cs149.ngram.Tokenizer;

public class Ngram {

    //Define custom input format for ngram
    public static class NgramInputFormat extends FileInputFormat<Text, Text>{
        public RecordReader<Text, Text> getRecordReader(InputSplit split, JobConf conf, Reporter reporter) throws IOException{
            reporter.setStatus(split.toString());
            return new PageRecordReader(conf, (FileSplit)split);
        }
    }

    public static class PageRecordReader implements RecordReader<Text, Text>{
        private Path file = null;
        private Configuration jc;
        private LineRecordReader lineReader;
        //private Text pageTitle;
        //private Text pageText;
        private Text spilloverTitle;
	    //For linereader
	    private LongWritable lineKey;
	    private Text lineValue;

        public PageRecordReader(JobConf conf, FileSplit split) throws IOException {
            FileSplit newSplit = (FileSplit) split;
            file = newSplit.getPath();
            jc = conf;
            lineReader = new LineRecordReader(jc, newSplit);
            //pageTitle = new Text("");
            //pageText = new Text("");
            spilloverTitle = new Text("");
		    lineKey = lineReader.createKey();
		    lineValue = lineReader.createValue();
        }

//        public void initialize (InputSplit split, JobConf job){
//         Do we need initialize method??/
//        }

        public static boolean isTitle(String currLine){
            if (currLine.contains("<title>"))
                return true;
            return false;
        }

        public static Text extractTitle(Text titleLine){
            int lineSize = titleLine.toString().length();
            String titleString = titleLine.toString();
            return new Text(titleString.substring(titleString.indexOf("<title>")+7,lineSize-8)); //check for off by 1!
        }
        public boolean next(Text key, Text value) throws IOException {
            String body = new String();
            while(true){
                boolean success = lineReader.next(lineKey, lineValue);
		System.out.println("Line key: " + lineKey.toString() + " Line Value: " + lineValue.toString());
                if(success){
                    //Text value = lineReader.getCurrentValue();
                    if (isTitle(lineValue.toString())){
                        key = spilloverTitle;
                        value = new Text(body);
                        spilloverTitle = extractTitle(lineValue);
			System.out.println("key " + key.toString() + " value " + value.toString());
                        return true; //done getting a title, body pair
                    }
                    body += lineValue.toString(); //append given line to body of text
                }else{
                   return false;
                }
            }
        }

        //@Override
        public Text createKey() {
            return new Text("");
        }

        //@Override
        public Text createValue() {
            return new Text("");
        }

        public long getPos() throws IOException {
                return lineReader.getPos();
        }

        public void close() throws IOException {
        }

        public float getProgress() throws IOException {
                return lineReader.getProgress();
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
                    fileContents.append(scanner.nextLine() + "\n");
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
                System.out.println("File read not successful");
            }
            Tokenizer tokenizer = new Tokenizer(fileContents);
            ArrayList<String> tempGram = new ArrayList<String>();
            while(tokenizer.hasNext()){
                tempGram.add(tokenizer.next());
                if(tempGram.size() == ngramSize){
                    String ngram = "";
                    for (String str: tempGram){ //Create string from tokens in arraylist
                        ngram += str;
                    }
                    allNgrams.add(ngram);
                    tempGram.remove(0); //Remove token at beginning
                }
            }
            return allNgrams;
        }


        //TODO: SHOULD WE BE CASE-SENSITIVE??
        //should we check if key is ""????
        public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            //Assume mapper gets (key, value) = (title page, text of page)
            if(key.toString().length()>0){
                int similarityScore = 0;
                HashSet<String> queryGrams = generateQueryNgrams();
                Tokenizer tokenizer = new Tokenizer(value.toString());
                ArrayList<String> tempGram = new ArrayList<String>();
                while(tokenizer.hasNext()){
                    tempGram.add(tokenizer.next());
                    if(tempGram.size() == ngramSize) {
                        String ngram = "";
                        for (String str : tempGram) { //Create string from tokens in arraylist
                            ngram += str;
                        }
                        if (queryGrams.contains(ngram)) {
                            similarityScore += 1;
                        }
                        tempGram.remove(0); //Remove token at beginning
                    }
                }

                String compositeValue = key.toString() + "," + Integer.toString(similarityScore);
                System.out.println("compositeValue: " + compositeValue);
                output.collect(new Text("1"), new Text(compositeValue));
            }
        }
    }

    //May have to define a custom Combiner class that emits 20 top pages from each mapper

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            int maxSim = 0;
            String bestPage = "";

            while(values.hasNext()){
                String[] pageScore = values.next().toString().split(","); //0 index = page title, 1 index = scorek
		        System.out.println("Array size: " + pageScore.length);
		        System.out.println("Array contents: " + Arrays.toString(pageScore));
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
//        conf.waitForCompletion(true);
    }
}
