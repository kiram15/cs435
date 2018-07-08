import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.net.URI;


public class Driver {

    public static class CountersClass {
        public enum UpdateCount {
            N
        }
    }

    static long numDocuments = -1;

    public static void main(String[] args) throws Exception {

        // -------------------------------- JOB 1 -----------------------------------
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "job1");
        job1.setJarByClass(Driver.class);
        job1.setNumReduceTasks(32);

        job1.setMapperClass(Job1.Job1Mapper.class);
        job1.setCombinerClass(Job1.Job1Reducer.class);
        job1.setReducerClass(Job1.Job1Reducer.class);
        job1.setPartitionerClass(Job1.Job1Partitioner.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path("intermediate_output1"));
        job1.waitForCompletion(true);

        // -------------------------------- JOB 2 -----------------------------------
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf, "job2");
        job2.setJarByClass(Driver.class);
        job2.setNumReduceTasks(32);

        job2.setMapperClass(Job2.Job2Mapper.class);
        job2.setReducerClass(Job2.Job2Reducer.class);
        job2.setPartitionerClass(Job2.Job2Partitioner.class);

        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path("intermediate_output1"));
        FileOutputFormat.setOutputPath(job2, new Path("intermediate_output2"));
        job2.waitForCompletion(true);

        Counter someCount = job2.getCounters().findCounter(CountersClass.UpdateCount.N);

        // -------------------------------- JOB 3 -----------------------------------
        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf, "job3");
        job3.setJarByClass(Driver.class);
        job3.setNumReduceTasks(32);

        job3.setMapperClass(Job3.Job3Mapper.class);
        job3.setCombinerClass(Job3.Job3Reducer.class);
        job3.setReducerClass(Job3.Job3Reducer.class);
        job3.setPartitionerClass(Job3.Job3Partitioner.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path("intermediate_output2"));
        FileOutputFormat.setOutputPath(job3, new Path("intermediate_output3"));
        job3.waitForCompletion(true);

        // -------------------------------- JOB 4 -----------------------------------
        Configuration conf4 = new Configuration();
        Job job4 = Job.getInstance(conf, "job4");
        job4.setJarByClass(Driver.class);
        job4.setNumReduceTasks(32);

        job4.getConfiguration().setLong(CountersClass.UpdateCount.N.name(), someCount.getValue());

        job4.setMapperClass(Job4.Job4Mapper.class);
        job4.setPartitionerClass(Job4.Job4Partitioner.class);

        job4.setOutputKeyClass(IntWritable.class);
        job4.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job4, new Path("intermediate_output3"));
        FileOutputFormat.setOutputPath(job4, new Path("intermediate_output4"));
        job4.waitForCompletion(true);

        // -------------------------------- JOB 5 -----------------------------------
        Configuration conf55 = new Configuration();
        Job job55 = Job.getInstance(conf, "job5");

        MultipleInputs.addInputPath(job55, new Path("intermediate_output4"),
                TextInputFormat.class, Job5New.Job5Mapper2.class);

        MultipleInputs.addInputPath(job55, new Path(args[0]), TextInputFormat.class,
                Job5New.Job5Mapper1.class);

        job55.setJarByClass(Driver.class);
        job55.setNumReduceTasks(32);

        //job55.setMapperClass(Job5New.Job5Mapper.class);
        job55.setReducerClass(Job5New.Job5Reducer.class);
        job55.setPartitionerClass(Job5New.Job5Partitioner.class);

        job55.setOutputKeyClass(IntWritable.class);
        job55.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job55, new Path(args[0]));
        FileOutputFormat.setOutputPath(job55, new Path("intermediate_output5"));
        System.exit(job55.waitForCompletion(true) ? 0 : 1);
    }
}
