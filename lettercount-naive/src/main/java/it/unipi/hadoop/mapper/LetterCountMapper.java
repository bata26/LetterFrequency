package it.unipi.hadoop.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LetterCountMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text count = new Text("TOTAL");

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        char[] chars = value.toString().toLowerCase().toCharArray();
        
        long counter = 0;
        for (char c : chars) {
            if (c >= 'a' && c <= 'z') {
                counter++;
            }
        }

        IntWritable counterWritable = new IntWritable((int) counter);
        context.write(count, counterWritable);
    }
}
