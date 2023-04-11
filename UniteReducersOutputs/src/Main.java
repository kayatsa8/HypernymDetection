import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {

    public static void main(String[] args) throws Exception {

        try{
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Unite");
            job.setJarByClass(MapperClass.class);
            job.setMapperClass(MapperClass.class);
            job.setPartitionerClass(PartitionerClass.class);
            //job.setCombinerClass(Combiner.class); --> the combiner is useless here
            job.setReducerClass(ReducerClass.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);
            job.setNumReduceTasks(1);
            FileInputFormat.addInputPath(job, new Path(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);

        }
        catch (Exception e){
            System.err.println("ERROR: " + e.getMessage() + "\n");
            System.err.println("Args length: " + args.length);
            System.err.println("args[0]: " + args[0]);
            System.err.println("args[1]: " + args[1]);
            System.err.println("\n\n");

            /*
                sometimes an error occurs with the code on try because the EMR
                puts the inputs in args[0] and args[1] instead args[1] and args[2],
                the catch allows the program to continue anyway.
            */
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Unite");
            job.setJarByClass(MapperClass.class);
            job.setMapperClass(MapperClass.class);
            job.setPartitionerClass(PartitionerClass.class);
            //job.setCombinerClass(Combiner.class); --> the combiner is useless here
            job.setReducerClass(ReducerClass.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);
            job.setNumReduceTasks(1);
            FileInputFormat.addInputPath(job, new Path("s3://yourfriendlyneighborhoodbucketman/extracted"));
            FileOutputFormat.setOutputPath(job, new Path("s3://yourfriendlyneighborhoodbucketman/united"));
            System.exit(job.waitForCompletion(true) ? 0 : 1);

        }




    }





}
