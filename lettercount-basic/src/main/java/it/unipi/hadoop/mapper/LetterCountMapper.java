package it.unipi.hadoop.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LetterCountMapper extends Mapper<Object, Text, Text, IntWritable> {


    private final Text letter = new Text("COUNT");

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        char[] chars = value.toString().toLowerCase().toCharArray();

        long count = 0;
        for (char c : chars) {
            if (Character.isLetter(c)) {
                count++;
            }
        }

        IntWritable countWritable = new IntWritable((int) count);
        context.write(letter, countWritable);
    }
    
}
