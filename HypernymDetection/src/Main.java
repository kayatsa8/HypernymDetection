public class Main {

    public static void main(String[] args) throws Exception {
        HypernymDetector hypernymDetector = new HypernymDetector();

        try{
            hypernymDetector.run();
        }
        catch(Exception e){
            System.out.println("An error occurred!");
            System.out.println(e.getMessage());
        }

    }



}
