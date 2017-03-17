import java.io.IOException;
import java.util.StringTokenizer;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
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

public class InvertedIndex {

    public static class LocationMapper
             extends Mapper<Object, Text, Text, Text>{

        private Text word = new Text();

        public void map(Object key, Text value, Context context
                                        ) throws IOException, InterruptedException {
            String [] lineObject = value.toString().split("\t");
            String lineNumberString = lineObject[0];
            String [] line = lineObject[1].split(" ");
            Text lineNumberText = new Text(lineNumberString);
            String wordString = new String();
            int d = line.length;
            int td = (int) Math.ceil(0.8*d);
            int firstWords = d - td + 1;
            for (int i = 0; i<firstWords; i++){
                wordString = line[i];
                word = new Text(wordString);
                if (lineNumberString.equals("4997")){
                    System.out.println(lineNumberString);
                    System.out.println(wordString);

                }
                context.write(word, lineNumberText);
            }
        }
    }

    public static class ConcatReducer
             extends Reducer<Text,Text,Text,Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context
                            ) throws IOException, InterruptedException {
            String list = "";
            for (Text val : values) {
                 String file = val.toString();
                 if (list == "") {
                         list = file;
                 } else {
                         list = list + ", " + file;
                }
            }
            Text result = new Text(list);
            context.write(key, result);
        }
    }



    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Inverted Index");
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(LocationMapper.class);
        job.setReducerClass(ConcatReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
