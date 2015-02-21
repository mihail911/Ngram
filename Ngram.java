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
import java.io.BufferedReader;
import java.io.FileReader;
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
        private LineRecordReader lineReader;
        private Text spilloverTitle;
	    //For linereader
	    private LongWritable lineKey;
	    private Text lineValue;

        public PageRecordReader(JobConf conf, FileSplit split) throws IOException {
            lineReader = new LineRecordReader(conf, split);
            spilloverTitle = new Text("");
		    lineKey = lineReader.createKey();
		    lineValue = lineReader.createValue();
        }

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
		        //System.out.println("Line key: " + lineKey.toString() + " Line Value: " + lineValue.toString());
                if(success){
                    //Text value = lineReader.getCurrentValue();
                    if (isTitle(lineValue.toString())){
                        key.set(spilloverTitle.toString());
                        value.set(body);
                        spilloverTitle = extractTitle(lineValue);
			            //System.out.println("key " + key.toString() + " value " + value.toString());
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

        private String query;
        private int ngramSize = 0;

        //Get parameters for Ngrams
        public void configure(JobConf job){
            query = job.get("query");
            ngramSize = Integer.parseInt(job.get("ngramSize"));
        }
	

        //TODO: SHOULD WE BE CASE-SENSITIVE??
        //should we check if key is ""????
        public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            //Assume mapper gets (key, value) = (title page, text of page)
            //System.out.println("Mapping key: " + key.toString());
            //System.out.println("Mapping value: " + value.toString());
            if(key.toString().length()>0){
                int similarityScore = 0;
                Tokenizer tokenizer = new Tokenizer(value.toString());
                ArrayList<String> tempGram = new ArrayList<String>();
                while(tokenizer.hasNext()){
                    tempGram.add(tokenizer.next());
                    if(tempGram.size() == ngramSize) {
                        String ngram = "";
                        for (String str : tempGram) { //Create string from tokens in arraylist
                            ngram += str;
                            ngram += " ";
                        }
                        if (query.contains(ngram)) {
                            similarityScore += 1;
                        }
                        tempGram.remove(0); //Remove token at beginning
                    }
                }

                String compositeValue = key.toString() + "|" + Integer.toString(similarityScore);
                System.out.println("compositeValue: " + compositeValue);
                output.collect(new Text("1"), new Text(compositeValue));
            }
        }
    }

	public static class KeyValuePair implements Comparable<KeyValuePair>{
		public int key;
		public String value;
		public KeyValuePair(int key, String value){
			this.key = key;
			this.value = value;
		}

		public int compareTo(KeyValuePair o){
			return key==o.key?(value.compareTo(o.value)):(o.key-key);
		}

	}

	public static class Combiner extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		TreeSet<KeyValuePair> scorePages = new TreeSet<KeyValuePair>();

            while(values.hasNext()){
                String[] pageScore = values.next().toString().split("\\|"); //0 index = page title, 1 index = scorek
//                        System.out.println("Array size: " + pageScore.length);
 //                       System.out.println("Array contents: " + Arrays.toString(pageScore));
		
                int score = Integer.parseInt(pageScore[1]);
		scorePages.add(new KeyValuePair(score, pageScore[0]));
            }
	
	String top20Pages = "";
	for(int count = 0; count < 20; count++){
		KeyValuePair page = scorePages.pollFirst();
		String stringPage = Integer.toString(page.key) + "#" +  page.value;
		top20Pages += (stringPage + "|");
	}	
	System.out.println("Top 20 in Combiner: " + top20Pages);
    output.collect(new Text("1"), new Text(top20Pages));

        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            int maxSim = 0;
            String bestPage = "";
			TreeSet<KeyValuePair> scorePages = new TreeSet<KeyValuePair>();

            while(values.hasNext()){
                String[] pageScore = values.next().toString().split("\\|"); //0 index = page title, 1 index = scorek
//		        System.out.println("Array size: " + pageScore.length);
//		        System.out.println("Array contents: " + Arrays.toString(pageScore));

				//All score Pages to key Set and then take top 20
				for(int index = 0; index < pageScore.length; index++){
					String[] entries = pageScore[index].split("\\#");
					scorePages.add(new KeyValuePair(Integer.parseInt(entries[0]), entries[1]));
				}

            }

    for(int count = 0; count < 20; count++){
        KeyValuePair page = scorePages.pollFirst();
		output.collect(new Text(Integer.toString(page.key)), new Text(page.value));
    }
        }
    }


    //read the File into a list of tokens
    public static String readFile(String pathname) throws IOException {
        BufferedReader in = new BufferedReader(new FileReader(pathname));
        String input = "";
        while(true) {
            String line = in.readLine();
            if (line == null)
                break;
            input += line;
            input += " ";
        }
        //return input;
        Tokenizer tokenizer = new Tokenizer(input);
        String query = "";
        while(tokenizer.hasNext()){
            query += tokenizer.next();
            query += " ";
        }
        System.out.println(query);
        return query;
    }


    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(Ngram.class);
        conf.setJobName("Ngram");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        //Set mapper output key,value types
        //conf.setMapOutputKeyClass(Text.class);
        //conf.setMapOutputValueClass(Text.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Combiner.class);
        conf.setReducerClass(Reduce.class);

        System.out.println("Input path is " + args[2] + ", Output path is " + args[3]);
        FileInputFormat.setInputPaths(conf, new Path(args[2]));
        FileOutputFormat.setOutputPath(conf, new Path(args[3]));

        conf.setInputFormat(NgramInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        //Set ngram parameters
        conf.set("ngramSize", args[0]);

        // TODO get query Ngrams
        String query = readFile(args[1]);
        conf.set("query",query);

        JobClient.runJob(conf);
        //if necessary
//        conf.waitForCompletion(true);
    }
}
