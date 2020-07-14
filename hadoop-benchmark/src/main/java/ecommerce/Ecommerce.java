package ecommerce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import utils.CFInputFormat;

import java.io.IOException;
import java.util.ArrayList;

public class Ecommerce {

    public static class Map extends Mapper<Object, Text, Text, DoubleWritable> {
        private final static IntWritable one = new IntWritable(1);
        private ArrayList<Object> mappedLine = new ArrayList<Object>();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] arrLine = line.split(",");
            Text event_time = new Text(arrLine[0]);
            mappedLine.add(event_time);
            Text event_type = new Text(arrLine[1].toUpperCase());
            mappedLine.add(event_type);
            Text product_id = new Text(arrLine[2]);
            mappedLine.add(product_id);
            Text category_id = new Text(arrLine[3]);
            mappedLine.add(category_id);
            Text category_code = new Text(arrLine[4]);
            mappedLine.add(category_code);

            String[] categorySplit = arrLine[4].split("\\.");
            Text category_general = null;
            if(!categorySplit[0].equals(""))
                category_general = new Text(categorySplit[0].toUpperCase());

            mappedLine.add(category_general);

            Text subcategory = null;
            if(categorySplit.length > 1)
                subcategory = new Text(categorySplit[1].toUpperCase());

            mappedLine.add(subcategory);

            Text brand = new Text(arrLine[5]);
            mappedLine.add(brand);

            DoubleWritable price;
            if(arrLine[6].equals(""))
                price = new DoubleWritable(0.0);
            else
                price = new DoubleWritable(Double.parseDouble(arrLine[6]));

            mappedLine.add(price);
            Text user_id = new Text(arrLine[7]);
            mappedLine.add(user_id);

            if(category_general == null && brand.getLength() != 0)
                context.write(brand, price);
            else if(category_general != null)
                context.write(category_general, price);
        }
    }

    public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
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

        job.setJarByClass(Ecommerce.class);
        job.waitForCompletion(true);
    }

}