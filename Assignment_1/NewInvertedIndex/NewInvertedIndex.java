import java.io.IOException;
import java.util.StringTokenizer;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.net.URI;
import org.apache.hadoop.io.SequenceFile;

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



public class NewInvertedIndex {

    public static class LocationMapper
             extends Mapper<Object, Text, Text, Text>{

        String stopWords = "";
        protected void setup(Context context) throws IOException, InterruptedException {
            String stopWordsPath = context.getConfiguration().get("stopWords");
            Path ofile = new Path(stopWordsPath);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br =new BufferedReader(new InputStreamReader(fs.open(ofile)));
            String line;
            line=br.readLine();
            try{
                while (line != null){
                    stopWords += line + ",";
                    line = br.readLine();
                    }
            } finally {
              br.close();
            }
        }

        private Text result = new Text();

        private Text word = new Text();

        public void map(Object key, Text value, Context context
                                        ) throws IOException, InterruptedException {
            String line = value.toString().toLowerCase();
            StringTokenizer itr = new StringTokenizer(line , " \t\n\r&\\-_!.;,()\"\'/:+=$[]§?#*|{}~0123456789<>@`%^");
            while (itr.hasMoreTokens()) {
                String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
                word.set(itr.nextToken());
                if (!stopWords.contains(word.toString())){
                    Text file = new Text(fileName);
                    context.write(word, file);
                }
            }
        }
    }


    public static class ConcatCombiner
             extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values,
                                             Context context
                                             ) throws IOException, InterruptedException {

                // Reset counters
                context.getCounter("nPerDoc", "pg100.txt").setValue(0);
                context.getCounter("nPerDoc", "pg31100.txt").setValue(0);
                context.getCounter("nPerDoc", "pg3200.txt").setValue(0);

                List<String> list = new ArrayList<String>();
                for (Text val : values) {
                     String file = val.toString();
                     if (list.isEmpty()) {
                             list.add(file);
                     }
                     if (!(list.contains(file))){
                        list.add(file);
                     }
                     context.getCounter("nPerDoc", file).increment(1);
                }

                ListIterator<String> it = list.listIterator();
                String s = "";
                while(it.hasNext()){
                    String str = it.next();
                    String fileFreq = str + "#" + String.valueOf(context.getCounter("nPerDoc", str).getValue());
                    if (s.equals("")) {
                        s = fileFreq;
                    } else {
                    s = s + "," + fileFreq;
                    }
                }

                Text result = new Text(s);
                context.write(key, result);
        }
    }

    public static class ConcatReducer
             extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values,
                                             Context context
                                             ) throws IOException, InterruptedException {

                int ndoc = 0;
                List<String> list = new ArrayList<String>();
                String file = "";
                int count = 0;
                for (Text val : values) {
                     String line = val.toString();
                     StringTokenizer itr = new StringTokenizer(line , ",#");
                     while (itr.hasMoreTokens()) {
                         file = itr.nextToken();
                         count = Integer.parseInt(itr.nextToken());
                         context.getCounter("nPerDocRed", file).increment(count);
                         if (!list.contains(file)){
                             list.add(file);
                         }
                     }
                 }
                ListIterator<String> it = list.listIterator();
                String s = "";
                while(it.hasNext()){
                    String str = it.next();
                    String fileFreq = str + "#" + String.valueOf(context.getCounter("nPerDocRed", str).getValue());
                    if (s.equals("")) {
                        s = fileFreq;
                    } else {
                    s = s + "," + fileFreq;
                    }
                }
                Text result = new Text(s);
                context.write(key, result);
            }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("stopWords", args[2]);
        Job job = Job.getInstance(conf, "Inverted Index");

        job.setJarByClass(NewInvertedIndex.class);
        job.setMapperClass(LocationMapper.class);
        job.setCombinerClass(ConcatCombiner.class);
        job.setReducerClass(ConcatReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
