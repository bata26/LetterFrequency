package it.unipi.hadoop.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Counter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import it.unipi.hadoop.mapper.*;

public class LetterFrequencyReducer extends Reducer<Text, IntWritable, Text, Text> {
    private Map<Text, Integer> countMap = new HashMap<>();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        countMap.put(new Text(key), sum);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Counter counter = context.getCounter(LetterFrequencyMapper.LetterCounter.TOTAL_LETTERS);
        long totalLetters = counter.getValue();

        for (Map.Entry<Text, Integer> entry : countMap.entrySet()) {
            Text letter = entry.getKey();
            int count = entry.getValue();
            double frequency = (double) count / totalLetters;
            context.write(letter, new Text(String.format("%.6f", frequency)));
        }
    }
}
