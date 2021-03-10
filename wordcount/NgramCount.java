import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import sun.awt.geom.AreaOp.SubOp;

public class NgramCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{
            
            private String lastLineLastStr = new String();
            private final static IntWritable one = new IntWritable(1);
            private Text word = new Text();

            public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                    Configuration conf = context.getConfiguration();
                    int n = Integer.parseInt(conf.get("Ngram"));

                    String str =  lastLineLastStr + ' ' + value.toString();
                    String[] arr = str.split("[^a-zA-Z0-9]+");
                    String[] subarr = new String[n];
                    String[] prevarr = new String[n-1];

                    int i;
                    for(i = (arr[0].isEmpty())? 1 : 0; i < arr.length - (n - 1); i++){
                        // join the sub-arrary to string
                        System.arraycopy(arr, i, subarr, 0, n);
                        word.set(String.join(" ", subarr));

                        context.write(word, one);
                    }
                    //set prevStr
                    System.arraycopy(arr, arr.length-n+1, prevarr, 0, n-1);
                    setLastStr(String.join(" ", prevarr));
                }

            public void setLastStr(String str){
                    // save the words for later use
                    this.lastLineLastStr = str;
            }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
            private IntWritable result = new IntWritable();

            public void reduce(Text key, Iterable<IntWritable> values,
                    Context context
                    ) throws IOException, InterruptedException {
                int sum = 0;
                for (IntWritable val : values) {
                    sum += val.get();
                }
                result.set(sum);
                context.write(key, result);
                }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("Ngram", args[2]);
        Job job = Job.getInstance(conf, "Ngram count");
        job.setJarByClass(NgramCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
