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

    static enum counters {APPEAR_IN_ONE_DOC_ONLY};

    public static class LocationMapper
             extends Mapper<Object, Text, Text, Text>{

        private Text word = new Text();

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

        public void map(Object key, Text value, Context context
                                        ) throws IOException, InterruptedException {
            String line = value.toString().toLowerCase();
            StringTokenizer itr = new StringTokenizer(line , " \t\n\r&\\-_!.;,()\"\'/:+=$[]§?#*|{}~0123456789<>@`");
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

    public static class ConcatReducer
             extends Reducer<Text,Text,Text,Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values,
                                             Context context
                                             ) throws IOException, InterruptedException {
            int ndoc = 0;
            String list = "";
            for (Text val : values) {
                 String file = val.toString();
                 if (list == "") {
                         list = file;
                         ndoc = 1;
                 }
                 if (!(list.contains(file))){
                         list = list + ", " + file;
                         ndoc+=1;
                }
            }
            if (ndoc == 1){
               context.getCounter(counters.APPEAR_IN_ONE_DOC_ONLY).increment(1);
            }
            Text result = new Text(list);
            context.write(key, result);
        }
    }



    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("stopWords", args[2]);
        Job job = Job.getInstance(conf, "Inverted Index");
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(LocationMapper.class);
        job.setReducerClass(ConcatReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

        Path pt = new Path("hdfs://localhost:9000/user/quentin/InvertedIndex/counter");
        FileSystem fs = pt.getFileSystem(conf);
        BufferedWriter bw=new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));
        long cnt = job.getCounters().findCounter(counters.APPEAR_IN_ONE_DOC_ONLY).getValue();
        bw.write("Number of words that appear only in one document : " + String.valueOf(cnt));
        bw.close();
    }
}
