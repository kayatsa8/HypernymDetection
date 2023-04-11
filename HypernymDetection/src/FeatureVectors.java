import com.opencsv.CSVWriter;

import java.io.*;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class FeatureVectors {

    private HashMap<String, Integer> patternVector; // <pattern, count>
    private HashMap<String,HashMap<String,Integer>> pairVectors; // <w1 w2, <pattern, count>>
    private HashMap<String, Boolean> pair_label; // <w1 w2, label>
    private String[] pairs;



    public FeatureVectors() throws Exception {
        patternVector = new HashMap<>();
        pairVectors = new HashMap<>();
        pair_label = new HashMap<>();
        pairs = null;
    }

    private List<String> readFile(File file) throws IOException {
        //Standard read lines from file , and store in List.
        List<String> array = new ArrayList<>();
        BufferedReader reader;
        reader = new BufferedReader(new FileReader(file));
        String line = reader.readLine();
        while (line != null) {
            array.add(line);
            line = reader.readLine();
        }
        reader.close();
        return array;
    }

    public void initialization(File file) throws Exception {
        List<String> data = readFile(file);
        //each string is of the form: "pattern \t\ count" or "w1 \d\ w2 \d\ label \d\ pattern \t\ \count\"
        for (String input: data) {
            if(!forPatternVector(input))
                if(!forPairVectors(input))
                    throw new Exception("(FeatureVectors) file from step4 is not well formed");
        }
        System.out.println("");
    }

    private boolean forPatternVector(String input){
        String[] arr = input.split("\\t");
        String[] split = arr[0].split(" ");
        if(split.length > 1)
            return false;
        String pattern = arr[0];
        Integer count = Integer.parseInt(arr[1]);
        patternVector.put(pattern,count);
        return true;
    }

    private boolean forPairVectors(String input){
        String[] arr = input.split("\\t");
        if(arr.length != 2)
            return false;

        String[] split = arr[0].split(" ");
        if(split.length != 4){
            return false;
        }

        String pair = split[0] + " " + split[1];
        boolean label = Boolean.parseBoolean(split[2]);
        String pattern = split[3];
        int count = Integer.parseInt(arr[1]);

        pairVectors.putIfAbsent(pair, new HashMap<>());
        pairVectors.get(pair).put(pattern, count);
        pair_label.putIfAbsent(pair, label);

        return true;
    }

    /**
     *  returns an array of all the patterns with at least t appearances
     */
    private String[] getMostSignificantPatterns(int t){
        List<String> msp = new ArrayList<>();

        for(String pattern : patternVector.keySet()){
            if(patternVector.get(pattern) >= t){
                msp.add(pattern);
            }
        }

        return msp.toArray(new String[0]);
    }

    private String[] getPairs(){
        pairs = new String[pairVectors.keySet().size()];
        int index = 0;

        for(String pair : pairVectors.keySet()){
            pairs[index] = pair;
            index++;
        }

        return pairs;

    }

    private int[][] getSampleVectors(String[] patterns, String[] pairs){
        int[][] samples = new int[pairs.length][patterns.length];
        String pair;
        String pattern;

        for(int i=0; i<samples.length; i++){
            for(int j=0; j< samples[0].length; j++){
                pair = pairs[i];
                pattern = patterns[j];

                if(pairVectors.get(pair).containsKey(pattern)){
                    samples[i][j] = pairVectors.get(pairs[i]).get(patterns[j]);
                }
                else{
                    samples[i][j] = 0;
                }

            }
        }

        return samples;
    }

    private String[] getLabels(String[] pairs){
        String[] labels = new String[pairs.length];

        for(int i=0; i<pairs.length; i++){
            if(pair_label.get(pairs[i])){
                labels[i] = "True";
            }
            else{
                labels[i] = "False";
            }
        }

        return labels;
    }

    /**
     *  t is the least number of appearances for pattern
     */
    public File makeCsvForClassifier(int t) throws IOException, URISyntaxException {
        String[] patterns = getMostSignificantPatterns(t);
        pairs = getPairs();
        int[][] samples = getSampleVectors(patterns, pairs);
        String[] labels = getLabels(pairs);
        return makeCsv(samples, labels);
    }

    private File makeCsv(int[][] samples, String[] labels){
        StringBuilder content = new StringBuilder();

        // titles
        for(int i=0; i<samples[0].length; i++){
            content.append("P").append(i).append(",");
        }
        content.append("label\n");

        // insert samples + labels
        for(int i=0; i<samples.length; i++){
            for(int j=0; j<samples[i].length; j++){
                content.append(samples[i][j]).append(",");
            }
            content.append(labels[i]).append("\n");
        }

        write2File(content.toString());

        return new File("Scsv.csv");
    }

    private void write2File(String content){
        // Create a new file if necessary
        try {
            File file = new File("Scsv.csv");
            file.createNewFile();
        }
        catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }

        // Write to the file
        try {
            FileWriter writer = new FileWriter("Scsv.csv");
            writer.write(content);
            writer.close();
        }
        catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }

    }

    public String[] PAIRS(){
        return Arrays.copyOf(pairs, pairs.length);
    }



}
