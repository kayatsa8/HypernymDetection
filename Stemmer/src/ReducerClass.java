import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

public class ReducerClass extends Reducer<Text, LongWritable, Text, LongWritable>{

    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        for(LongWritable value : values){
            context.write(key, value);
        }

        /*
            IMPORTANT: the values list should be in the length of 1,
            still we referred to it as a list with unknown size.
         */
    }

}
