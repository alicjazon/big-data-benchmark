package ecommerce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import utils.CFInputFormat;

import java.io.IOException;

public class EcommercePerformanceBench {

    public static class Map extends Mapper<Object, Text, Text, DoubleWritable> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] arrLine = line.split(",");

            double sum = 0;
            for(int i = 0; i <10000; i++){
                sum += i;
            }

            double sum2 = 0;
            for(int i = 0; i <20000; i++){
                sum2 += i;
            }

            String[] categorySplit = arrLine[4].split("\\.");
            Text category_general = null;
            if(!categorySplit[0].equals(""))
                category_general = new Text(categorySplit[0].toUpperCase());

            Text brand = new Text(arrLine[5]);

            DoubleWritable price;
            if(arrLine[6].equals(""))
                price = new DoubleWritable(0.0);
            else
                price = new DoubleWritable(sum/sum2);

            if(category_general == null && brand.getLength() != 0)
                context.write(brand, price);
            else if(category_general != null)
                context.write(category_general, price);
        }
    }

    public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            context.write(key, new DoubleWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        Job job = Job.getInstance(conf, "ecommerce");
        job.setNumReduceTasks(4);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(CFInputFormat.class);

        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        job.setJarByClass(EcommercePerformanceBench.class);
        job.waitForCompletion(true);
    }

}