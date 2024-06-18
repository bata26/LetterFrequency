package it.unipi.hadoop.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LetterCountMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text letter = new Text();
    public enum Counters {
        TOTAL_LETTERS
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        char[] chars = value.toString().toLowerCase().toCharArray();

        for (char c : chars) {
            if (c >= 'a' && c <= 'z') {
                letter.set(Character.toString(c));
                context.write(letter, one);
                context.getCounter(LetterCountMapper.Counters.TOTAL_LETTERS).increment(1);
            }
        }
    }

}
