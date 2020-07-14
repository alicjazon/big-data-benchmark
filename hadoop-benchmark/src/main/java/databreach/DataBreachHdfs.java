package databreach;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import utils.CFInputFormat;
import utils.CFRecordReader;

import java.io.IOException;

public class DataBreachHdfs {

    public static class MapEmail extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] arrLine = line.split(",");

            Text directory = new Text(CFRecordReader.fileName);
            Text email = new Text(arrLine[1]);
            context.write(email, directory);
        }
    }

    public static class MapPassword extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] arrLine = line.split(",");

            Text directory = new Text(CFRecordReader.fileName);

            Text password = new Text(arrLine[3]);
            context.write(password, directory);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = Job.getInstance(conf, "DataBreach");
        FileSystem fs = FileSystem.get(conf);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(MapEmail.class);
        job.setInputFormatClass(CFInputFormat.class);

        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        job.setJarByClass(DataBreachHdfs.class);
        job.waitForCompletion(true);

        // JOB 2
        Configuration conf2 = new Configuration();

        Job job2 = Job.getInstance(conf2, "DataBreach2");

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setMapperClass(MapPassword.class);
        job2.setInputFormatClass(CFInputFormat.class);
        FileInputFormat.setInputDirRecursive(job2, true);
        FileInputFormat.addInputPath(job2, new Path(otherArgs[0]));

        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));

        job2.setJarByClass(DataBreachHdfs.class);
        job2.waitForCompletion(true);
    }

}