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


public class SetSimilarityJoins {

    static enum counters {NUMBER_OF_COMPARISONS};

    public static class SetSimilarityJoinsMapper
             extends Mapper<Object, Text, Text, Text>{

        private int maxLine = 5000;

        private IntWritable lineNumber = new IntWritable(0);

        private HashMap<String, String> hm = new HashMap<String, String>();

        protected void setup(Context context) throws IOException, InterruptedException {
            // Store the inverted index in a HashMap
            String IIPath = context.getConfiguration().get("InvertedIndex");
            Path ofile = new Path(IIPath);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br =new BufferedReader(new InputStreamReader(fs.open(ofile)));
            String line;
            line=br.readLine();
            try{
                while (line != null){
                    String [] keyvalue = line.split("\t");
                    String word = keyvalue[0];
                    String indexes = keyvalue[1];
                    hm.put(word, indexes);
                    line = br.readLine();
                }
            } finally {
              br.close();
            }
        }

        public void map(Object key, Text value, Context context
                                        ) throws IOException, InterruptedException {
            lineNumber.set(lineNumber.get()+1);
            if (lineNumber.get() < maxLine){
                String [] inLine = value.toString().split("\t");
                String lineNumberString = inLine[0];
                int lineNumber = Integer.parseInt(inLine[0]);
                String [] line = inLine[1].split(" ");
                Text lineText = new Text(inLine[1]);
                
                // Compute the number of words to consider
                int d = line.length;
                int td = (int) Math.ceil(0.8*d);
                int firstWords = d - td + 1;

                Set<String> linesToCompare =  new HashSet<String>() ;

                // Loop on the words to consider and get lines associated
                // in the inverted index
                for (int i = 0; i<firstWords; i++){
                    String word = line[i];
                    String [] otherLines = hm.get(word).split(", ");
                    for (String otherLine:otherLines){
                        int otherLineInt = Integer.parseInt(otherLine);
                        if (otherLineInt < maxLine)
                            linesToCompare.add(otherLine);
                    }
                }

                // Create obtained pairs keys and output them to the reducer
                for(String otherLineString : linesToCompare){
                    int otherLine = Integer.parseInt(otherLineString);
                    String newKey = new String();
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
            
            List<String> valuesArray = new ArrayList<String>(2);
            for (Text value : values){
                valuesArray.add(value.toString());
            }
            if (valuesArray.size() == 2){
                String s1 = valuesArray.get(0);
                String s2 = valuesArray.get(1);
                String [] v1 = s1.split(" ");
                String [] v2 = s2.split(" ");

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

                if (sim > 0.8){
                    String out = s1 + " // " + s2 + " // sim = " + String.valueOf(sim);
                    Text value = new Text(out); 
                    context.write(key, value);
                }

            context.getCounter(counters.NUMBER_OF_COMPARISONS).increment(1);

            } else {
                System.out.println(key.toString());
                System.out.println(valuesArray);
                System.out.println();
            }
        }
    }



    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("InvertedIndex", args[2]);
        Job job = Job.getInstance(conf, "SetSimilarityJoins");
        job.setJarByClass(SetSimilarityJoins.class);
        job.setMapperClass(SetSimilarityJoinsMapper.class);
        job.setReducerClass(ComparisonReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
