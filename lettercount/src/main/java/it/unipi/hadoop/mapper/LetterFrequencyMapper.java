package it.unipi.hadoop.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LetterFrequencyMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text letter = new Text();

    public enum LetterCounter {
        TOTAL_LETTERS
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().toLowerCase().trim();
        if (!line.isEmpty()) {
            for (char ch : line.toCharArray()) {
                if (Character.isLetter(ch)) {
                    letter.set(Character.toString(ch));
                    context.write(letter, one);
                    context.getCounter(LetterCounter.TOTAL_LETTERS).increment(1);
                }
            }
        }
    }
}
