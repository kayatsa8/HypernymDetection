import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.*;
import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.ArffSaver;
import weka.core.converters.CSVLoader;
import weka.classifiers.functions.LibSVM;
import weka.core.converters.ConverterUtils;

import java.io.*;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * In short:
 * Step 1: MapReduce Stemmer on n-grams.
 * Step 2: M-R to join n-grams to hypernyms.
 * Step 3: unites all Step 2 outputs.
 * Step 4: creates feature-vectors for training.
 * Step 5: train the predictor
 * Step 6: test the predictor
*/

public class HypernymDetector {

    EmrClient emr;
    FeatureVectors featureVectors;
    S3_Service s3_service;



    public HypernymDetector(){
        emr = EmrClient.builder().credentialsProvider(ProfileCredentialsProvider.create()).build();
        s3_service = S3_Service.getInstance();
    }

    public void run() throws Exception {

        uploadAllFiles();

        //Create JobFlow
        String clusterId = makeJobFlow();

        //Step1
        stem(clusterId);

        //Step2
        extractPatterns(clusterId);

        //Step3
        uniteReducersOutput(clusterId);

        //Step4 creation of feature vectors from extracted data presented in step3's result file.
        //feature vectors in FeatureVectors object.
        makeFeatureVectors();

        //Step5
        runClassifier();
    }

    private void uploadAllFiles(){
        String hypernymPath = "hypernym.txt";
        uploadFile(hypernymPath);
        uploadFile("nGrams");
        uploadFile("Stemmer.jar");
        uploadFile("ExtractPatterns.jar");
        uploadFile("UniteReducersOutputs.jar");
    }

    private void stem(String clusterId) throws InterruptedException {
        String inputAddress = "s3://yourfriendlyneighborhoodbucketman/nGrams",
                outputAddress = "s3://yourfriendlyneighborhoodbucketman/stemmed";
        AddJobFlowStepsResponse stemmer_response = addStep(clusterId,
                "stemmer_step" ,
                "s3://yourfriendlyneighborhoodbucketman/Stemmer.jar",
                inputAddress,
                outputAddress);
        if(!checkStepTerminated(stemmer_response,clusterId))
            throw new InterruptedException("Exception message on step1");
    }

    private String makeJobFlow(){
        JobFlowInstancesConfig jobFlowInstancesConfig = JobFlowInstancesConfig.builder()
                .instanceCount(8)
                .masterInstanceType(InstanceType.M4_LARGE.toString())
                .slaveInstanceType(InstanceType.M4_LARGE.toString())
                .hadoopVersion("5.36.0")
                .ec2KeyName("vockey")
                .keepJobFlowAliveWhenNoSteps(true) //maybe change.
                .placement(PlacementType.builder().availabilityZone("us-east-1a").build()).build();

        RunJobFlowRequest runFlowRequest = RunJobFlowRequest.builder()
                .name("hadoopmenicourse")
                .instances(jobFlowInstancesConfig)
                .jobFlowRole("EMR_EC2_DefaultRole")
                .releaseLabel("emr-5.36.0")
                .serviceRole("EMR_DefaultRole")
                .jobFlowRole("LabInstanceProfile")
                .logUri("s3://yourfriendlyneighborhoodbucketman/logs/")
                .build();
        RunJobFlowResponse runJobFlowResponse = emr.runJobFlow(runFlowRequest);
        String clusterId; //cluster id = jobflow id
        do{
            clusterId = runJobFlowResponse.jobFlowId();
            try{
                TimeUnit.SECONDS.sleep(2);
            }
            catch(Exception e){
                System.err.println("ERROR:\n");
                System.err.println(e.getMessage());
                System.exit(1);
            }
        }while (clusterId == null);
        System.out.println("ClusterId = " + clusterId);
        return clusterId;
    }

    private AddJobFlowStepsResponse addStep(String clusterId, String stepName , String jarAddress, String inputAddress , String outputAddress){
        /*
        1.example of jarAddress = "s3://yourfriendlyneighborhoodbucketman/Step1.jar"
        2.example of inputAddress = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data"
        3.example of outputAddress = "s3://yourfriendlyneighborhoodbucketman/somefile.txt"
        * */
        HadoopJarStepConfig hadoopJarStepConfig = HadoopJarStepConfig.builder()
                .jar(jarAddress)
                .mainClass("Main")
                .args(inputAddress, outputAddress).build();
        StepConfig stepConfig1 = StepConfig.builder()
                .name(stepName)
                .actionOnFailure("CONTINUE") //TERMINATE_JOB_FLOW
                .hadoopJarStep(hadoopJarStepConfig).build();
        AddJobFlowStepsResponse stepResponse= emr.addJobFlowSteps(AddJobFlowStepsRequest.builder()
                .steps(stepConfig1)
                .jobFlowId(clusterId)
                .build());
        return stepResponse;
    }

    private void uploadFile(String fileName) {
        try{
            s3_service.makeKey(fileName);
            s3_service.uploadInputToS3(fileName, fileName);
        }
        catch(Exception e){
            System.err.println("ERROR: can't upload file to server!\n");
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    private boolean checkStepTerminated(AddJobFlowStepsResponse stepResponse, String clusterId) throws InterruptedException {
        List<String> id;
        boolean stop = false;
        DescribeStepResponse res;
        StepStatus status;
        StepState stepState = null;
        String state = null;
        do{
            TimeUnit.SECONDS.sleep(60);
            id = stepResponse.stepIds();
            if(id.size() != 0){
                res = emr.describeStep(DescribeStepRequest.builder().clusterId(clusterId).stepId(id.get(0)).build());
                status = res.step().status();
                stepState = status.state();
                state = stepState.name();
                stop = (state.equals("COMPLETED") || state.equals("FAILED") || state.equals("INTERRUPTED"));
            }
        }while(!stop);
        if(state.equals("COMPLETED")){
            System.out.println("checkStepTerminated: true" + stepState.name());
            return true;
        }
        else
            return false;
    }

    private void extractPatterns(String clusterId) throws InterruptedException {
        AddJobFlowStepsResponse extractPatterns_response = addStep(clusterId,
                "extractPatterns_step",
                "s3://yourfriendlyneighborhoodbucketman/ExtractPatterns.jar",
                "s3://yourfriendlyneighborhoodbucketman/stemmed",
                "s3://yourfriendlyneighborhoodbucketman/extracted");
        if(!checkStepTerminated(extractPatterns_response,clusterId))
            throw new InterruptedException("Exception message on step2");
    }

    private void uniteReducersOutput(String clusterId) throws InterruptedException {
        AddJobFlowStepsResponse uniteReducerOutputs_response = addStep(clusterId,
                "uniteReducerOutputs_step",
                "s3://yourfriendlyneighborhoodbucketman/UniteReducersOutputs.jar",
                "s3://yourfriendlyneighborhoodbucketman/extracted",
                "s3://yourfriendlyneighborhoodbucketman/united");
        if(!checkStepTerminated(uniteReducerOutputs_response,clusterId))
            throw new InterruptedException("Exception message on step3");
    }

    private void makeFeatureVectors() throws Exception {
        String nameInS3 = "united/part-r-00000";
        String nameInSystem = "united";
        File dataFile = s3_service.downloadFromS3(nameInS3, nameInSystem);
        featureVectors = new FeatureVectors();
        featureVectors.initialization(dataFile);
    }

    private void runClassifier() throws IOException, URISyntaxException {
        File Scsv = featureVectors.makeCsvForClassifier(3); // after experiments, 3 gave us the best results

        try{
            File S = csv2arff(Scsv);
            Instances train = ConverterUtils.DataSource.read(S.getAbsolutePath()); // X
            train.setClassIndex(train.numAttributes()-1); // Y
            LibSVM svm = new LibSVM();
            svm.buildClassifier(train);
            Evaluation eval = new Evaluation(train);
            eval.crossValidateModel(svm, train, 10, new Random(1));
            printResults(eval, train.numAttributes()-1);
            confusionTable(svm, train);
        }
        catch(Exception e){
            System.out.println("ERROR:\n");
            System.out.println(e.getMessage());
        }
    }

    private File csv2arff(File csv) throws Exception {
        // load csv
        CSVLoader loader = new CSVLoader();
        loader.setSource(csv);
        Instances data = loader.getDataSet();

        // save arff
        ArffSaver saver = new ArffSaver();
        saver.setInstances(data);
        File S = new File("S.arff");
        saver.setFile(S);
        saver.writeBatch();

        return S;
    }

    private void printResults(Evaluation eval, int classIndex){
        double precision = (eval.precision(0) + eval.precision(1))/2;
        double recall = (eval.recall(0) + eval.recall(1))/2;
        double f1 = (eval.fMeasure(0) + eval.fMeasure(1))/2;

        System.out.println("Precision: " + precision);
        System.out.println("Recall: " + recall);
        System.out.println("F1: " + f1);
    }

    private void confusionTable(Classifier classifier, Instances instances) throws Exception {
        int[] truePositive = new int[5];
        int[] falsePositive = new int[5];
        int[] trueNegative = new int[5];
        int[] falseNegative = new int[5];
        Arrays.fill(truePositive, -1);
        Arrays.fill(falsePositive, -1);
        Arrays.fill(trueNegative, -1);
        Arrays.fill(falseNegative, -1);

        int tpIndex = 0, fpIndex = 0, tnIndex = 0, fnIndex = 0;

        Instance s;
        String trueLabel, predictedLabel;

        for(int i=0; i<instances.numInstances(); i++){
            s = instances.instance(i);
            trueLabel = getLabel(s, instances.classIndex());
            predictedLabel = getPredictedLabel(classifier, s, instances);

            if(tpIndex < 5 && trueLabel.equals("True") && predictedLabel.equals("True")){
                truePositive[tpIndex] = i;
                tpIndex++;
            }
            if(fpIndex < 5 && trueLabel.equals("False") && predictedLabel.equals("True")){
                falsePositive[fpIndex] = i;
                fpIndex++;
            }
            if(tnIndex < 5 && trueLabel.equals("False") && predictedLabel.equals("False")){
                trueNegative[tnIndex] = i;
                tnIndex++;
            }
            if(fnIndex < 5 && trueLabel.equals("True") && predictedLabel.equals("False")){
                falseNegative[fnIndex] = i;
                fnIndex++;
            }

            if(tpIndex == 5 && fpIndex == 5 && tnIndex == 5 && fnIndex == 5){
                break;
            }

        }

        String content = makeConfusionString(truePositive, falsePositive, trueNegative, falseNegative, instances);
        write2File(content);

    }

    private String getLabel(Instance s, int classIndex){
        return s.toString(classIndex);
    }

    private String getPredictedLabel(Classifier classifier, Instance s, Instances S) throws Exception {
        double c = classifier.classifyInstance(s);
        return s.classAttribute().value((int)c);
    }

    private String makeConfusionString(int[] truePositive, int[] falsePositive, int[] trueNegative, int[] falseNegative, Instances S){
        String[] pairs = featureVectors.PAIRS();
        String content = "True positive:\n";

        for(int i=0; i<5; i++){
            if(truePositive[i] != -1){
                content += pairs[truePositive[i]] + ": " + S.instance(truePositive[i]).toString() + "\n";
            }
        }

        content += "\n";
        content += "False positive:\n";

        for(int i=0; i<5; i++){
            if(falsePositive[i] != -1){
                content += pairs[falsePositive[i]] + ": " + S.instance(falsePositive[i]).toString() + "\n";
            }
        }

        content += "\n";
        content += "True negative:\n";

        for(int i=0; i<5; i++){
            if(trueNegative[i] != -1){
                content += pairs[trueNegative[i]] + ": " + S.instance(trueNegative[i]).toString() + "\n";
            }
        }

        content += "\n";
        content += "False negative:\n";

        for(int i=0; i<5; i++){
            if(falseNegative[i] != -1){
                content += pairs[falseNegative[i]] + ": " + S.instance(falseNegative[i]).toString() + "\n";
            }
        }

        return content;
    }

    private void write2File(String content){
        // Create a new file if necessary
        try {
            File file = new File("confusionTable.txt");
            file.createNewFile();
        }
        catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }

        // Write to the file
        try {
            FileWriter writer = new FileWriter("confusionTable.txt");
            writer.write(content);
            writer.close();
        }
        catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }

    }



}
