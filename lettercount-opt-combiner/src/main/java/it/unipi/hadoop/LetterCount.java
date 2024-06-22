package it.unipi.hadoop;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import it.unipi.hadoop.mapper.*;
import it.unipi.hadoop.reducer.*;


public class LetterCount {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: LetterFrequencyDriver <input path> <output path> <number of reducers>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "letter frequency count");
        job.setJarByClass(LetterCount.class);
        job.setMapperClass(LetterFrequencyMapper.class);
        job.setCombinerClass(LetterFrequencyCombiner.class);
        job.setReducerClass(LetterFrequencyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        FileSystem hdfs = FileSystem.get(conf);
        org.apache.hadoop.fs.Path path = new Path(args[1]);
        if (hdfs.exists(path))
          hdfs.delete(path, true);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        int numReducers = Integer.parseInt(args[2]);
        job.setNumReduceTasks(numReducers);
        
        double startTime = System.nanoTime();
        int exitStatus = job.waitForCompletion(true) ? 0 : 1;
        Double executionTime = (System.nanoTime() - startTime) / 1000000000.0;
        byte[] executionTimeStr = executionTime.toString().getBytes();
        java.nio.file.Path dirPath = Paths.get(".", "timing", args[1]);
        if (!Files.exists(dirPath)) {
            Files.createDirectories(dirPath);
        }
        
        // File path for the specific timing result
        java.nio.file.Path filePath = Paths.get(".", "timing", args[1] + "/result.txt");
        Files.write(filePath, executionTimeStr, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        if (exitStatus == 1) {
            System.exit(1);
        }
    }
}
