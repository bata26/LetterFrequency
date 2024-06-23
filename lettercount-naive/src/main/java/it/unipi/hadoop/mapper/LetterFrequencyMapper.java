package it.unipi.hadoop.mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class LetterFrequencyMapper extends Mapper<Object, Text, Text, IntWritable> {
    private static Map<Character, Integer> countLetters;


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
                countLetters.put(c, countLetters.getOrDefault(c, 1) + 1);
            }
        }
    }


    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<Character, Integer> entry : countLetters.entrySet()) {
            context.write(new Text(entry.getKey().toString()), new IntWritable(entry.getValue()));
        }
    }
}