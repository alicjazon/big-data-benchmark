package pi;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Pi extends Configured implements Tool {
    static private final String TMP_DIR_PREFIX = Pi.class.getSimpleName();
    static private long start;

    public static class QmcMapper extends
            Mapper<LongWritable, LongWritable, BooleanWritable, LongWritable> {

        public void map(LongWritable offset,
                        LongWritable size,
                        Context context)
                throws IOException, InterruptedException {
            long numInside = 0L;
            long numOutside = 0L;

            for(long i = 0; i < size.get(); ) {
                final double x = Math.random() * 2 - 1;
                final double y = Math.random() * 2 - 1;
                if (x*x + y*y > 1) {
                    numOutside++;
                } else {
                    numInside++;
                }

                i++;
                if (i % 1000 == 0) {
                    context.setStatus("Generated " + i + " samples.");
                }
            }

            context.write(new BooleanWritable(true), new LongWritable(numInside));
            context.write(new BooleanWritable(false), new LongWritable(numOutside));
        }
    }

    public static class QmcReducer extends
            Reducer<BooleanWritable, LongWritable, WritableComparable<?>, Writable> {

        private long numInside = 0;
        private long numOutside = 0;

        public void reduce(BooleanWritable isInside,
                           Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            if (isInside.get()) {
                for (LongWritable val : values) {
                    numInside += val.get();
                }
            } else {
                for (LongWritable val : values) {
                    numOutside += val.get();
                }
            }
        }

        @Override
        public void cleanup(Context context) throws IOException {
            //write output to a file
            Configuration conf = context.getConfiguration();
            Path outDir = new Path(conf.get(FileOutputFormat.OUTDIR));
            Path outFile = new Path(outDir, "reduce-out");
            FileSystem fileSys = FileSystem.get(conf);
            SequenceFile.Writer writer = SequenceFile.createWriter(fileSys, conf,
                    outFile, LongWritable.class, LongWritable.class,
                    CompressionType.NONE);
            writer.append(new LongWritable(numInside), new LongWritable(numOutside));
            writer.close();
        }
    }

    public static BigDecimal estimatePi(int numMaps, long numPoints,
                                        Path tmpDir, Configuration conf
    ) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = new Job(conf);

        job.setJobName(Pi.class.getSimpleName());
        job.setJarByClass(Pi.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setOutputKeyClass(BooleanWritable.class);
        job.setOutputValueClass(LongWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapperClass(QmcMapper.class);

        job.setReducerClass(QmcReducer.class);
        job.setNumReduceTasks(1);
        job.setSpeculativeExecution(false);

        final Path inDir = new Path(tmpDir, "in");
        final Path outDir = new Path(tmpDir, "out");
        FileInputFormat.setInputPaths(job, inDir);
        FileOutputFormat.setOutputPath(job, outDir);

        final FileSystem fs = FileSystem.get(conf);

        try {
            for(int i=0; i < numMaps; ++i) {
                final Path file = new Path(inDir, "part"+i);
                final LongWritable offset = new LongWritable(i * numPoints);
                final LongWritable size = new LongWritable(numPoints);
                final SequenceFile.Writer writer = SequenceFile.createWriter(
                        fs, conf, file,
                        LongWritable.class, LongWritable.class, CompressionType.NONE);
                try {
                    writer.append(offset, size);
                } finally {
                    writer.close();
                }
                System.out.println("Wrote input for Map #"+i);
            }

            System.out.println("Starting Job");
            final long startTime = System.currentTimeMillis();
            job.waitForCompletion(true);
            final double duration = (System.currentTimeMillis() - startTime)/1000.0;
            System.out.println("Job Finished in " + duration + " seconds");

            Path inFile = new Path(outDir, "reduce-out");
            LongWritable numInside = new LongWritable();
            LongWritable numOutside = new LongWritable();
            SequenceFile.Reader reader = new SequenceFile.Reader(fs, inFile, conf);
            try {
                reader.next(numInside, numOutside);
            } finally {
                reader.close();
            }

            final BigDecimal numTotal
                    = BigDecimal.valueOf(numMaps).multiply(BigDecimal.valueOf(numPoints));
            return BigDecimal.valueOf(4).setScale(20)
                    .multiply(BigDecimal.valueOf(numInside.get()))
                    .divide(numTotal, RoundingMode.HALF_UP);
        } finally {
            fs.delete(tmpDir, true);
        }
    }
    public int run(String[] args) throws Exception {
        final int nMaps = Integer.parseInt(args[0]);
        final long nSamples = Long.parseLong(args[1]);
        long now = System.currentTimeMillis();
        int rand = new Random().nextInt(Integer.MAX_VALUE);
        final Path tmpDir = new Path(TMP_DIR_PREFIX + "_" + now + "_" + rand);
        start = System.nanoTime();
        System.out.println("Estimated value of Pi is " + estimatePi(nMaps, nSamples, tmpDir, getConf()));
        long end = System.nanoTime();
        long elapsedTime = end - start;
        System.out.println("Elapsed time " + elapsedTime + " ns");
        return 0;
    }
    public static void main(String[] argv) throws Exception {
        System.exit(ToolRunner.run(null, new Pi(), argv));
    }
}