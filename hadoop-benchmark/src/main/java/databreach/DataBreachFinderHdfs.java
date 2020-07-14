package databreach;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import utils.CFInputFormat;

import java.io.IOException;

public class DataBreachFinderHdfs {
    static Text input;

    public static class Map extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] arrLine = line.split(",");

            Text directory = new Text(arrLine[0]);
            Text check = new Text(arrLine[1]);
            if (check.equals(input))
                context.write(check, directory);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = Job.getInstance(conf, "DataBreach");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        input = new Text(args[2]);
        job.setMapperClass(Map.class);

        job.setInputFormatClass(CFInputFormat.class);

        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        job.setJarByClass(DataBreachFinderHdfs.class);
        job.waitForCompletion(true);
    }

}