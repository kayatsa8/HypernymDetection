import org.apache.hadoop.io.LongWritable;

import java.util.*;

public class SentenceNgram {

    //frail/JJ/ccomp/3 we/PRP/nsubj/3 are/VBP/dep/0 \tab\ 12
    private LongWritable count;
    private Word[] words;
    private boolean[][] tree; // the n-gram tree
    private String sentence;


    //Initialization
    public SentenceNgram(String sentence){
        this.sentence = sentence;
    }

    public void build() throws Exception {
        extractCount(sentence);
        extractNgrams(sentence);
        makeTree();
    }

    private void extractCount(String sentence){
        String[] arr = sentence.split("\\t");
        count = new LongWritable(Long.parseLong(arr[1]));
    }

    private void extractNgrams(String sentence) throws Exception {
        String[] temp = sentence.split("\\t");
        String[] arr = temp[0].split(" ");
        words = new Word[arr.length];
        try{
            for(int i=0 ; i<arr.length ; i++){
                words[i] = new Word(arr[i]);
                words[i].build();
            }
        }
        catch (Exception e){
            throw new Exception("Bad n-gram!");
        }
    }

    public HashMap<String,String> getPatternsOfNounPairs() throws Exception {
        //Find patterns of all noun pairs in the sentence.
        String key1, key2, pattern1, pattern2;
        if(words == null)
            throw new Exception("(SentenceNgram) ngrams was not initialized");
        HashMap<String,String> nounsPatterns = new HashMap<>();
        for(int i = 0; i< words.length ; i++)
            for(int j = i+1; j< words.length ; j++){
                if(words[i].isNoun() && words[j].isNoun()){
                    key1 = words[i].getWord() + " " + words[j].getWord();
                    key2 = words[j].getWord() + " " + words[i].getWord();
                    pattern1 = getPattern(i,j);
                    pattern2 = getPattern(j, i);
                    nounsPatterns.putIfAbsent(key1,pattern1);
                    nounsPatterns.putIfAbsent(key2,pattern2);
                }
            }
        return nounsPatterns;
    }

    private String getPattern(int word1, int word2) throws Exception {

        if(word1 < 0 || word1 > words.length-1 || word2 < 0 || word2 > words.length-1)
            throw new Exception("(SentenceNgram) index is out of bounds in getPattern()");

        List<Integer> shortestPath = shortestPath(word1, word2);

        return makeDependencyPath(shortestPath);

    }

    public LongWritable getCount(){
        return count;
    }

    private void makeTree(){
        int predecessor;

        initTree();

        for(int i = 0; i< words.length; i++){
            predecessor = words[i].getPredecessor();
            if(predecessor > 0){
                tree[i][predecessor-1] = true;
                tree[predecessor-1][i] = true;
            }
        }

    }

    private void initTree(){
        tree = new boolean[words.length][words.length];

        for(int i=0; i<tree.length; i++){
            for(int j=0; j<tree[0].length; j++){
                tree[i][j] = false;
            }
        }
    }

    /**
        Adjusted BFS, finds the shortest path from src to dst
     */
    private int[] bfs(int src, int dst){
        int[] paths = new int[words.length];
        Queue<Integer> queue = new ArrayDeque<>();
        int curr;
        List<Integer> neighbors;

        Arrays.fill(paths, -1);
        queue.add(src);
        paths[src] = 0;

        while(paths[dst] == -1 && queue.size() != 0){
            curr = queue.poll();
            neighbors = getNeighbors(curr);
            for(int neighbor : neighbors){
                if(paths[neighbor] == -1){
                    paths[neighbor] = curr;
                    queue.add(neighbor);
                }
            }
        }

        return paths;

    }

    private List<Integer> getNeighbors(int vertex){
        List<Integer> neighbors = new ArrayList<>();

        for(int i=0; i<tree[vertex].length; i++){
            if(tree[vertex][i]){
                neighbors.add(i);
            }
        }

        return neighbors;
    }

    private List<Integer> constructPath(int src, int dst, int[] paths){
        List<Integer> path = new ArrayList<>();
        int curr = dst;

        while(curr != src){
            path.add(0, curr);
            curr = paths[curr];
        }

        path.add(0, src);

        return path;
    }

    private List<Integer> shortestPath(int src, int dst){
        int[] paths = bfs(src, dst);
        return constructPath(src, dst, paths);
    }

    private String makeDependencyPath(List<Integer> shortestPath){
        String dependencyPath = "";
        int currWord, nextWord;

        for(int i=0; i<shortestPath.size()-1; i++){
            currWord = shortestPath.get(i);
            nextWord = shortestPath.get(i+1);
            if(isPredecessor(currWord, nextWord)){
                dependencyPath += makeDependencyNode(currWord, nextWord) + "_";
            }
            else{
                dependencyPath += makeDependencyNode(nextWord, currWord) + "_";
            }
        }

        dependencyPath = dependencyPath.substring(0, dependencyPath.length()-1);

        return dependencyPath;

    }

    /**
        returns true if prev is the predecessor of curr
     */
    private boolean isPredecessor(int curr, int prev){
        return words[curr].getPredecessor()-1 == prev;
    }

    private String makeDependencyNode(int word, int predecessor){
        return words[word].getPartOfSpeech() +
                "/" +
                words[word].getDependencyLabel() +
                "/" +
                words[predecessor].getPartOfSpeech();
    }



}
