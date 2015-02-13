/* This will implement the actual map/reduce operations.
*/

import java.io.IOException;
import java.lang.String;
import java.util.*;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import Tokenizer;

public class Ngram {
    //Define custom input format for ngram
//    public static class NgramInputFormat extends FileInputFormat<Text, Text>{
//        public RecordReader<Text, Text> getRecordReader(InputSplit input, JobConf job, Reporter reporter) throws IOException{
//            reporter.setStatus(input.toString());
//            return new PageRecordReader(job, (FileSplit)input);
//        }
//    }



    public static class Map extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
        private final static IntWritable one = new IntWritable(1);

        private String queryFile;
        private int ngramSize = 0;

        //Get parameters for Ngrams
        public void configure(JobConf job){
            queryFile = job.get("queryFile");
            ngramSize = String.valueOf(jobs.get("ngramSize"));
        }


        //Convert contents of file into a string
        static String readFile(String path, Charset encoding)
                throws IOException
        {
            byte[] encoded = Files.readAllBytes(Paths.get(path));
            return new String(encoded, encoding);
        }

        //Creates set of all ngrams in query document --maybe want to make this generic for the pages as well??
        public HashSet<String> generateQueryNgrams(){
            HashSet<String> allNgrams = new HashSet<String>();
            String fileContents = readFile(queryFile, Charset.defaultCharset());
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
            Tokenizer pageTokenizer = Tokenizer(value.toString());
            ArrayList<String> tempGram = new ArrayList<String>();
            while(pageTokenizer.hasNext()){
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

            /*String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                output.collect(word, one);
            }*/

        }
    }

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
            /*
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
            */
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

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        //Set ngram parameters
        conf.set("ngramSize", Integer.toString(args[0]));
        conf.set("queryFile", args[1]);

        FileInputFormat.setInputPaths(conf, new Path(args[2]));
        FileOutputFormat.setOutputPath(conf, new Path(args[3]));

        JobClient.runJob(conf);
    }
}