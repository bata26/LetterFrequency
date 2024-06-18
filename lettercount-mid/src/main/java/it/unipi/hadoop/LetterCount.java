package it.unipi.hadoop;

import it.unipi.hadoop.mapper.LetterCountMapper;
import it.unipi.hadoop.reducer.LetterCountReducer;
import it.unipi.hadoop.combiner.LetterCountCombiner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.Counters;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;

public class LetterCount {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
           System.err.println("Usage: LetterCount <input> <output> <reduce_task");
           System.exit(1);
        }
        System.out.println("args[0]: <input>="  + otherArgs[0]);
        System.out.println("args[1]: <output>=" + otherArgs[1]);
        System.out.println("args[2]: <reduce_task>=" + otherArgs[2]);

        Job job = Job.getInstance(conf, "lettercount-mid");
        job.setJarByClass(LetterCount.class);
        job.setMapperClass(LetterCountMapper.class);
        job.setCombinerClass(LetterCountCombiner.class);
        job.setReducerClass(LetterCountReducer.class);
        
        // specify the mapper output format
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // specify the reducer output format
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // specify the number of reducers
        int numReducers = Integer.parseInt(args[2]);
        job.setNumReduceTasks(numReducers);


        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileSystem hdfs = FileSystem.get(conf);
        org.apache.hadoop.fs.Path path = new Path(otherArgs[1]);
        if (hdfs.exists(path))
          hdfs.delete(path, true);
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        double startTime = System.nanoTime();
        boolean jobCompleted = job.waitForCompletion(true);
        
        if (jobCompleted) {
            Counters counters = job.getCounters();
            long totalLetters = counters.findCounter(LetterCountMapper.Counters.TOTAL_LETTERS).getValue();

            System.out.println("Total letters: " + totalLetters);

            Path outputPath = new Path(args[1]);
            FileSystem fs = FileSystem.get(conf);
            FileStatus[] status = fs.listStatus(outputPath);
            Map<String, Integer> letterCounts = new HashMap<>();

            for (FileStatus fileStatus : status) {
                if (fileStatus.getPath().getName().startsWith("part-")) {
                    FSDataInputStream inputStream = fs.open(fileStatus.getPath());
                    BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] parts = line.split("\t");
                        String letter = parts[0];
                        int count = Integer.parseInt(parts[1]);
                        letterCounts.put(letter, letterCounts.getOrDefault(letter, 0) + count);
                    }

                    reader.close();
                }
            }

            org.apache.hadoop.fs.Path pathFreq = new Path(otherArgs[1]);
            if (hdfs.exists(path))
                hdfs.delete(path, true);
            FSDataOutputStream outputStream = fs.create(pathFreq);
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream));
            for (Map.Entry<String, Integer> entry : letterCounts.entrySet()) {
                String letter = entry.getKey();
                int count = entry.getValue();
                double frequency = (double) count / totalLetters;
                writer.write(letter + frequency);
                writer.newLine();
            }
            // calculate execution time
            Double executionTime = (System.nanoTime() - startTime) / 1000000000.0;
            byte[] executionTimeStr = executionTime.toString().getBytes();
            java.nio.file.Path dirPath = Paths.get(".", "timing", args[1]);
            if (!Files.exists(dirPath)) {
                Files.createDirectories(dirPath);
            }

            // File path for the specific timing result
            java.nio.file.Path filePath = Paths.get(".", "timing", args[1] + "/result.txt");
            Files.write(filePath, executionTimeStr, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        }
    }
}