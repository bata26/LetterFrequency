package it.unipi.hadoop.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.HashMap;
import java.util.Map;

public class LetterCountMapper extends Mapper<Object, Text, Text, IntWritable> {

    //private final Text letter = new Text("COUNT");
    private final static IntWritable one = new IntWritable(1);
    private Text letter = new Text();
    private Map<Character, Integer> charFrequencyMap = new HashMap<>();
    public enum Counters {
        TOTAL_LETTERS
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        char[] chars = value.toString().toLowerCase().toCharArray();

        for (char c : chars) {
            if (Character.isLetter(c)) {
                letter.set(Character.toString(c));
                context.write(letter, one);
                context.getCounter(Counters.TOTAL_LETTERS).increment(1);
            }
        }
    }

}
