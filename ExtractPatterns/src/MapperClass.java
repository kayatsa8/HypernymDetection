import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import software.amazon.awssdk.utils.IoUtils;

import java.io.*;
import java.net.URI;
import java.util.*;

public class MapperClass extends Mapper<LongWritable, Text, Text, LongWritable> {

    private final HashMap<String, Boolean> hypernyms = new HashMap<>(); // <w1 w2, label>
    private StringBuilder F = new StringBuilder();


    @Override
    protected void setup(Context context) throws IOException {
        /*
        1.download file named "train_hypernym" from s3
        2.pairs of words to a list
        */
        String bucket = "yourfriendlyneighborhoodbucketman1";
        FileSystem fileSystem = FileSystem.get(URI.create("s3://"+ bucket),context.getConfiguration());
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path("s3://" + bucket + "/hypernym.txt"));
        String input = IoUtils.toUtf8String(fsDataInputStream);
        fsDataInputStream.close();
        fileSystem.close();
        fileToHashMap(input);
    }

    /**
     * output: <pattern, count>
     *         <<word1 word2 label pattern>, count>
     */
    public void map(LongWritable lineID, Text text, Context context) throws IOException, InterruptedException {
        String pattern;
        String label;
        SentenceNgram sentenceNgram = new SentenceNgram(text.toString());
        try {
            sentenceNgram.build();
            //Get hashtable of all noun pairs in the sentence.
            HashMap<String,String> hashMap =  sentenceNgram.getPatternsOfNounPairs();
            //remove those who don't appear in hypernym
            for (String key: hashMap.keySet()){
                if(hypernyms.containsKey(key)){
                    //emit
                    pattern = hashMap.get(key);

                    if(hypernyms.get(key)){
                        label = "True";
                    }
                    else{
                        label = "False";
                    }

                    String toWrite = key + " " + label + " " + pattern;
                    context.write(new Text(pattern), sentenceNgram.getCount());
                    context.write(new Text(toWrite) , sentenceNgram.getCount());
                }
            }
        }
        catch (Exception e) {
            if(e.getMessage() != null)
                if(!e.getMessage().equals("Bad n-gram!"))
                    System.out.println(e.getMessage());
        }
    }

    private void fileToHashMap(String content) {
        String[] split = content.split("\\n");
        String[] record;

        for(String r : split){
            record = r.split("\\t");
            hypernyms.put(record[0] + " " + record[1], record[2].contains("True")); // <w1 w2, label>
        }
    }

}
