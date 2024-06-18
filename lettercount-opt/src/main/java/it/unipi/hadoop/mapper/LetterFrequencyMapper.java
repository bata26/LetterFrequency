package it.unipi.hadoop.mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LetterFrequencyMapper extends Mapper<Object, Text, Text, IntWritable> {
    private static Map<String, Integer> countLetters;
    public enum LetterCounter {
        TOTAL_LETTERS
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        countLetters = new HashMap<>();
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        char[] chars = value.toString().toLowerCase().toCharArray();

        for (char c : chars) {
            if (c >= 'a' && c <= 'z') {
                countLetters.put(Character.toString(c), 1);
                context.getCounter(LetterCounter.TOTAL_LETTERS).increment(1);
            }
        }
    }


    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, Integer> entry : countLetters.entrySet()) {
            context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
        }
    }
}
