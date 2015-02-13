/* This will implement the actual map/reduce operations.
*/

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Ngram {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        private String queryFile;
        private int ngramSize = 0;

        //TODO: read in contents of queryinput File

        //Get parameters for Ngrams
        public void configure(JobConf job){
            queryFile = job.get("queryFile");
            ngramSize = String.valueOf(jobs.get("ngramSize"));
        }

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            System.out.println(value.toString());
            /*String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                output.collect(word, one);
            }*/

        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
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