package it.unipi.hadoop.mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class LetterFrequencyMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Map<Character, Integer> charFrequencyMap = new HashMap<>();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        char[] chars = value.toString().toLowerCase().toCharArray();

        for (char c : chars) {
            if (c >= 'a' && c <= 'z') {
                charFrequencyMap.put(c, charFrequencyMap.getOrDefault(c, 1) + 1);
            }
        }

        // Emit each entry in the charFrequencyMap
        for (Map.Entry<Character, Integer> entry : charFrequencyMap.entrySet()) {
            context.write(new Text(entry.getKey().toString()), new IntWritable(entry.getValue()));
        }
    }
}