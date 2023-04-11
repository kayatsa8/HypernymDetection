import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class MapperClass extends Mapper<LongWritable, Text, Text, LongWritable>{

    /**
     * value can be in the form of [pattern \t count] or [w1 \d w2 \d label \d pattern \t count]
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(value == null) {
            return;
        }

        String[] split = value.toString().split("\\t");
        String k = split[0];
        long v = Long.parseLong(split[1]);

        context.write(new Text(k), new LongWritable(v));

    }

}
