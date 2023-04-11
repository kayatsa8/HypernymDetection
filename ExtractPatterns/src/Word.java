public class Word {


    private String word; // frail
    private String partOfSpeech; // JJ
    private String dependencyLabel; // ccmop
    private int predecessorPosition; // 3
    private String ngram;


    public Word(String ngram){
        this.ngram = ngram;
    }

    public void build(){
        String[] nGramParts = ngram.split("/");
        word = nGramParts[0];
        partOfSpeech = nGramParts[1];
        dependencyLabel = nGramParts[2];
        predecessorPosition = Integer.parseInt(nGramParts[3]);
    }


    public boolean isNoun(){
        if(partOfSpeech.equals("NN") || partOfSpeech.equals("NNS") || partOfSpeech.equals("NNP") || partOfSpeech.equals("NNPS"))
            return true;
        return false;
    }

    public String getWord(){
        return word;
    }

    public String getPartOfSpeech(){
        return partOfSpeech;
    }

    public String getDependencyLabel(){
        return dependencyLabel;
    }

    public String getNgramPattern(){
        return partOfSpeech + "/" + dependencyLabel;
    }

    public int getPredecessor(){
        return predecessorPosition;
    }


}
