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

public class PreProcessing {

    final static IntWritable lineNumber = new IntWritable(0);

    public static class PreProcessingMapper
             extends Mapper<Object, Text, IntWritable, Text>{

        private Text wordText = new Text();
        private String word = new String();


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
            String inLine = value.toString();
            String outLine = "";
            StringTokenizer itr = new StringTokenizer(inLine , " \t\n\r&\\-_!.;,()\"\'/:+=$[]§?#*|{}~<>@`");
            while (itr.hasMoreTokens()) {
                wordText.set(itr.nextToken());
                word = wordText.toString();
                if (!stopWords.contains(word) && !outLine.contains(word)){
                    outLine = outLine + word + " ";
                }
            }
            if (outLine.length()>0) {
                lineNumber.set(lineNumber.get()+1);
                outLine = outLine.substring(0, outLine.length()-1);
                Text outLineText = new Text(outLine);
                context.write(lineNumber, outLineText);
            }
        }
    }

    public static class OrderReducer
             extends Reducer<IntWritable,Text,IntWritable,Text> {
        
        private HashMap<String, Integer> hm = new HashMap<String, Integer>();

        protected void setup(Context context) throws IOException, InterruptedException {
            String wordCountPath = context.getConfiguration().get("wordCount");
            Path ofile = new Path(wordCountPath);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br =new BufferedReader(new InputStreamReader(fs.open(ofile)));
            String line;
            line=br.readLine();
            try{
                while (line != null){
                    String [] keyvalue = line.split("\t");
                    String word = keyvalue[0];
                    int count = Integer.parseInt(keyvalue[1]);
                    hm.put(word, count);
                    line = br.readLine();
                }
            } finally {
              br.close();
            }
        }

        public void reduce(IntWritable key, Iterable<Text> values,
                                             Context context
                                             ) throws IOException, InterruptedException {

            for (Text value : values){
                // Get line's words and associated counts
                String line = value.toString();
                StringTokenizer itr = new StringTokenizer(line);
                HashMap<String, Integer> subhm = new HashMap<String, Integer>();
                while (itr.hasMoreTokens()) {
                    String word = itr.nextToken();
                    subhm.put(word, hm.get(word));
                }

                // Order as a function of count
                List<String> mapKeys = new ArrayList<String>(subhm.keySet());
                List<Integer> mapValues = new ArrayList<Integer>(subhm.values());
                Collections.sort(mapValues);

                LinkedList<String> list = new LinkedList<String>();
    
                Iterator<Integer> valueIt = mapValues.iterator();
                while (valueIt.hasNext()) {
                    Integer val = valueIt.next();
                    Iterator<String> keyIt = mapKeys.iterator();
                    while (keyIt.hasNext()) {
                        String sortedword = keyIt.next();
                        Integer comp1 = subhm.get(sortedword);
                        Integer comp2 = val;
                        if (comp1.equals(comp2)) {
                            keyIt.remove();
                            list.add(sortedword);
                        }
                    }
                }

                String sortedString  = String.join(" ", list);
                Text result = new Text(sortedString); 
                context.write(key, result);
            }
        }
    }



    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("stopWords", args[2]);
        conf.set("wordCount", args[3]);
        Job job = Job.getInstance(conf, "PreProcessing");
        job.setJarByClass(PreProcessing.class);
        job.setMapperClass(PreProcessingMapper.class);
        job.setReducerClass(OrderReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

        Path pt = new Path("hdfs://localhost:9000/user/quentin/PreProcessing/lineNumber");
        FileSystem fs = pt.getFileSystem(conf);
        BufferedWriter bw=new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));
        bw.write(String.valueOf(lineNumber));
        bw.close();
    }
}
