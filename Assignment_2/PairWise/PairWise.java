import java.io.IOException;
import java.util.StringTokenizer;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.util.*;
// import java.util.ArrayList;
// import java.util.Collections;
// import java.util.List;
// import java.util.LinkedList;
// import java.util.ListIterator;
// import java.util.HashMap;
import java.net.URI;
import java.lang.Math;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Counter;


public class PairWise {

    static enum counters {NUMBER_OF_COMPARISONS};

    public static class PairWiseMapper
             extends Mapper<Object, Text, Text, Text>{

        private int totalLineNumber;

        private int maxLine = 5000;

        private IntWritable lineNumber = new IntWritable(0);


        public void setup(Context context) throws IOException, InterruptedException {
            String lineNumberPath = context.getConfiguration().get("lineNumber");
            Path ofile = new Path(lineNumberPath);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br =new BufferedReader(new InputStreamReader(fs.open(ofile)));
            String totalLineNumberString = br.readLine();
            totalLineNumber = Integer.parseInt(totalLineNumberString);
            br.close();

            // Bound its value to consider only a subset and reduce calculation
            totalLineNumber = Math.min(totalLineNumber, maxLine);
        }

        public void map(Object key, Text value, Context context
                                        ) throws IOException, InterruptedException {
            lineNumber.set(lineNumber.get()+1);
            if (lineNumber.get() <= totalLineNumber){
                String [] inLine = value.toString().split("\t");
                String lineNumberString = inLine[0];
                int lineNumber = Integer.parseInt(inLine[0]);
                String line = inLine[1];
                Text lineText = new Text(line);
                
                // Generate pairs of treated line with all other lines
                for (int otherLine = 1; otherLine<=totalLineNumber; otherLine++){
                    String newKey = new String();
                    String otherLineString = String.valueOf(otherLine);
                    if (!(otherLine == lineNumber)) {
                        if (lineNumber < otherLine){
                            newKey = lineNumberString + "," + otherLineString;
                        } else {
                            newKey = otherLineString + "," + lineNumberString;
                        }
                        Text newKeyText = new Text(newKey);
                        context.write(newKeyText, lineText);
                    }
                }
            }
        }
    }


    public static class ComparisonReducer
             extends Reducer<Text,Text,Text,Text> {

            public void reduce(Text key, Iterable<Text> values,
                                             Context context
                                             ) throws IOException, InterruptedException {
            
            // Put all lines associated with the key in an array
            List<String> valuesArray = new ArrayList<String>(2);
            for (Text value : values){
                valuesArray.add(value.toString());
            }

            // Make sure that the same pair of documents is compared no more than once
            if (valuesArray.size() == 2){
                String s1 = valuesArray.get(0);
                String s2 = valuesArray.get(1);
                String [] v1 = s1.split(" ");
                String [] v2 = s2.split(" ");

                // Jaccard similarity
                int inter = 0;
                int union = 0;

                for (String word : v1){
                    if (s2.contains(word)){
                        inter++;
                        union++;
                    } 
                    else 
                        union++;
                }

                for (String word : v2){
                    if (!(s1.contains(word)))
                        union++;
                }

                float sim = (float) inter / union;

                // Output pairs that are more similar than a given threshold
                if (sim > 0.8){
                    String out = s1 + " // " + s2 + " // sim = " + String.valueOf(sim);
                    Text value = new Text(out); 
                    context.write(key, value);
                }
                context.getCounter(counters.NUMBER_OF_COMPARISONS).increment(1);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("lineNumber", args[2]);
        Job job = Job.getInstance(conf, "PairWise");
        job.setJarByClass(PairWise.class);
        job.setMapperClass(PairWiseMapper.class);
        job.setReducerClass(ComparisonReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
