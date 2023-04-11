import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerClass extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    public void reduce(Text key, Iterable<LongWritable> counts, Context context) throws IOException, InterruptedException {
        long count = 0;

        for(LongWritable c : counts){
            count += c.get();
        }
        context.write(key, new LongWritable(count));
    }

}
