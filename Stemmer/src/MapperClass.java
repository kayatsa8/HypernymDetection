import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import edu.stanford.nlp.process.Morphology;


public class MapperClass extends Mapper<LongWritable, Text, Text, LongWritable>{

    private final int N_GRAM_POSITION = 1;
    private final int TOTAL_COUNTS_POSITION = 2;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(value == null) {
            return;
        }

        String[] nGrams = getNgrams(value.toString());

        stem(nGrams);

        int totalCounts = getTotalCounts(value.toString());

        String reunitedNGrams = "";

        for(String nGram : nGrams){
            reunitedNGrams += nGram;
            reunitedNGrams += " ";
        }

        reunitedNGrams = reunitedNGrams.substring(0, reunitedNGrams.length()-1);

        context.write(new Text(reunitedNGrams), new LongWritable(totalCounts));

    }

    private String[] getNgrams(String value){
        String[] afterSplit = value.split("\\t");
        return afterSplit[N_GRAM_POSITION].split(" ");
    }

    private void stem(String[] nGrams){
        String[] temp;

        for(int i = 0; i<nGrams.length; i++){
            temp = nGrams[i].split("/");
            Morphology morphology = new Morphology();
            temp[0] = morphology.stem(temp[0]);
            nGrams[i] = temp[0] + "/" + temp[1] + "/" + temp[2] + "/" + temp[3];
        }

    }

    private int getTotalCounts(String value){
        String[] afterSplit = value.split("\\t");
        return Integer.parseInt(afterSplit[TOTAL_COUNTS_POSITION]);
    }

}
