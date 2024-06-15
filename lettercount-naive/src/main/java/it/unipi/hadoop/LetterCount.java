package it.unipi.hadoop;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import it.unipi.hadoop.mapper.*;
import it.unipi.hadoop.reducer.*;
import it.unipi.hadoop.combiner.*;

public class LetterCount {
    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: LetterFrequencyDriver <input path> <output path count> <output path frequency> <number of reducers>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job countJob = Job.getInstance(conf, "letter count");
        countJob.setJarByClass(LetterCount.class);
        countJob.setMapperClass(LetterCountMapper.class);
        countJob.setCombinerClass(LetterCountCombiner.class);
        countJob.setReducerClass(LetterCountReducer.class);

        countJob.setOutputKeyClass(Text.class);
        countJob.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(countJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(countJob, new Path(args[1]));

        int numReducers = Integer.parseInt(args[3]);
        countJob.setNumReduceTasks(numReducers);

        int countStatus = countJob.waitForCompletion(true) ? 0 : 1;

        if (countStatus == 1) {
            System.exit(1);
        }

        // Read the total letters count from the output of the first job
        Path countOutputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] status = fs.listStatus(countOutputPath);

        int totalLetters = 0;

        for (FileStatus fileStatus : status) {
            if (fileStatus.getPath().getName().startsWith("part-")) {
                FSDataInputStream inputStream = fs.open(fileStatus.getPath());
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\t");
                    if (parts[0].equals("TOTAL")) {
                        totalLetters = Integer.parseInt(parts[1]);
                        break;
                    }
                }

                reader.close();
                inputStream.close();
            }
        }

        System.out.println("Total letters: " + totalLetters);

        // Set the total letters count in the configuration
        conf.setInt("TOTAL", totalLetters);

        Job freqJob = Job.getInstance(conf, "letter frequency");
        freqJob.setJarByClass(LetterCount.class);
        freqJob.setMapperClass(LetterFrequencyMapper.class);
        freqJob.setCombinerClass(LetterFrequencyCombiner.class);
        freqJob.setReducerClass(LetterFrequencyReducer.class); // Assuming you have a reducer that calculates the frequency

        freqJob.setOutputKeyClass(Text.class);
        freqJob.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(freqJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(freqJob, new Path(args[2])); // Output path for frequency results

        freqJob.setNumReduceTasks(numReducers);

        System.exit(freqJob.waitForCompletion(true) ? 0 : 1);
    }
}