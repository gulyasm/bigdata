package hu.dmlab.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class WcMapper extends  org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, IntWritable> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if(line.length() == 0 ) {
            return;
        }
        String[] words = line.split(" ");
        for(String word: words) {
            context.write(new Text(word), new IntWritable(1));
        }
    }
}
