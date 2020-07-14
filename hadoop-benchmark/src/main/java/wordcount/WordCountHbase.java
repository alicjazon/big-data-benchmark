package wordcount;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class WordCountHbase {
    public static long start;

    public static class Map extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().replaceAll("[,.+!+?;/:„”—\"%'\\-\\[\\]–)(*…]", "").toLowerCase();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class Reduce extends TableReducer<Text, IntWritable, Text> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            start = System.nanoTime();
            Put put = new Put(toBytes(key.toString()));
            put.addColumn(toBytes("number"), toBytes(""), toBytes(sum));
            context.write(null, put);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        new ArrayUtils();

        Job job = Job.getInstance(conf, "wordcount");

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        TableMapReduceUtil.initTableReducerJob(
                "wordcount",
                Reduce.class,
                job);

        job.setJarByClass(WordCountHbase.class);
        job.waitForCompletion(true);
        long end = System.nanoTime();
        long elapsedTime = end - start;
        System.out.println("Elapsed time " + elapsedTime + " ns " + elapsedTime/1000000000);
    }

}