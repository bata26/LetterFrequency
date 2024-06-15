package it.unipi.hadoop.reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import it.unipi.hadoop.mapper.*;

public class LetterFrequencyReducer extends Reducer<Text, IntWritable, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }

        Configuration conf = context.getConfiguration();
        Cluster cluster = new Cluster(conf);
        Job currentJob = cluster.getJob(context.getJobID());
        long totalLetters = currentJob.getCounters().findCounter(LetterFrequencyMapper.LetterCounter.TOTAL_LETTERS).getValue(); 

        double frequency = (double) sum / totalLetters;
        context.write(key, new Text(String.format("%.6f", frequency)));
    }
}
