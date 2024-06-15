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
    private long totalLetters;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        totalLetters = context.getConfiguration().getLong("TOTAL", 1);
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }

        double frequency = (double) sum / totalLetters;
        context.write(key, new Text(String.format("%.6f", frequency)));
    }
}
