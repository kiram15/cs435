import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Profile3 {

    public static class TokenizerMapper1 extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (value.toString().contains("<====>")) {
                StringTokenizer itr = new StringTokenizer(value.toString().split("<====>")[2]);
                while (itr.hasMoreTokens()) {
                    String curr = itr.nextToken();
                    curr = curr.replaceAll("[^A-Za-z0-9 ]", "").toLowerCase();
                    if (!curr.equals("")) {
                        word.set(curr);
                        context.write(word, one);
                    }
                }
            }
        }
    }

    public static class WCPartitioner1 extends Partitioner<Text, IntWritable> {
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            if (numReduceTasks == 27) {
                Character partitionKey = key.toString().toLowerCase().charAt(0);
                if (partitionKey == 'a')
                    return 1;
                else if (partitionKey == 'b')
                    return 2;
                else if (partitionKey == 'c')
                    return 3;
                else if (partitionKey == 'd')
                    return 4;
                else if (partitionKey == 'e')
                    return 5;
                else if (partitionKey == 'f')
                    return 6;
                else if (partitionKey == 'g')
                    return 7;
                else if (partitionKey == 'h')
                    return 8;
                else if (partitionKey == 'i')
                    return 9;
                else if (partitionKey == 'j')
                    return 10;
                else if (partitionKey == 'k')
                    return 11;
                else if (partitionKey == 'l')
                    return 12;
                else if (partitionKey == 'm')
                    return 13;
                else if (partitionKey == 'n')
                    return 14;
                else if (partitionKey == 'o')
                    return 15;
                else if (partitionKey == 'p')
                    return 16;
                else if (partitionKey == 'q')
                    return 17;
                else if (partitionKey == 'r')
                    return 18;
                else if (partitionKey == 's')
                    return 19;
                else if (partitionKey == 't')
                    return 20;
                else if (partitionKey == 'u')
                    return 21;
                else if (partitionKey == 'v')
                    return 22;
                else if (partitionKey == 'w')
                    return 23;
                else if (partitionKey == 'x')
                    return 24;
                else if (partitionKey == 'y')
                    return 25;
                else if (partitionKey == 'z')
                    return 26;
                else //for numbers
                    return 0;
            }
            return 0;
        }
    }

    public static class IntSumReducer1 extends Reducer<Text,IntWritable,Text,IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class TokenizerMapper2 extends Mapper<Object, Text, IntWritable, Text> {
        private Text word = new Text();
        IntWritable frequency = new IntWritable();


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] info = value.toString().split("\\t");
            word.set(info[0]);
            int i = Integer.parseInt(info[1]);
            frequency.set(i);
            context.write(frequency, word);
        }
    }

    public static class WCPartitioner2 extends Partitioner<IntWritable, Text> {
        public int getPartition(IntWritable key, Text value, int numReduceTasks) {
            if (numReduceTasks == 28) {
                int partitionKey = key.get();
                System.out.println("****************************" + partitionKey + "****************************" );
                if (partitionKey == 1)
                    return 27;
                else if (partitionKey == 2)
                    return 26;
                else if (partitionKey >= 3 && partitionKey <= 6)
                    return 25;
                else if (partitionKey >= 7 && partitionKey <= 11)
                    return 24;
                else if (partitionKey >= 12 && partitionKey <= 15)
                    return 23;
                else if (partitionKey >= 16 && partitionKey <= 20)
                    return 22;
                else if (partitionKey >= 21 && partitionKey <= 31)
                    return 21;
                else if (partitionKey >= 32 && partitionKey <= 43)
                    return 20;
                else if (partitionKey >= 44 && partitionKey <= 54)
                    return 19;
                else if (partitionKey >= 55 && partitionKey <= 75)
                    return 18;
                else if (partitionKey >= 76 && partitionKey <= 110)
                    return 17;
                else if (partitionKey >= 111 && partitionKey <= 160)
                    return 16;
                else if (partitionKey >= 161 && partitionKey <= 200)
                    return 15;
                else if (partitionKey >= 201 && partitionKey <= 315)
                    return 14;
                else if (partitionKey >= 316 && partitionKey <= 500)
                    return 13;
                else if (partitionKey >= 501 && partitionKey <= 700)
                    return 12;
                else if (partitionKey >= 701 && partitionKey <= 950)
                    return 11;
                else if (partitionKey >= 951 && partitionKey <= 1200)
                    return 10;
                else if (partitionKey >= 1201 && partitionKey <= 1500)
                    return 9;
                else if (partitionKey >= 1501 && partitionKey <= 1900)
                    return 8;
                else if (partitionKey >= 1901 && partitionKey <= 2000)
                    return 7;
                else if (partitionKey >= 2001 && partitionKey <= 3000)
                    return 6;
                else if (partitionKey >= 3001 && partitionKey <= 4000)
                    return 5;
                else if (partitionKey >= 4001 && partitionKey <= 5000)
                    return 4;
                else if (partitionKey >= 5001 && partitionKey <= 6000)
                    return 3;
                else if (partitionKey >= 6001 && partitionKey <= 7000)
                    return 2;
                else if (partitionKey >= 7001 && partitionKey <= 8000)
                    return 1;
                else
                    return 0;
            }
            return 0;
        }
    }

    public static class IntSumReducer2 extends Reducer<IntWritable,Text,IntWritable,Text> {
        Text word = new Text();
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text words : values) {
                word.set(words);
                context.write(key, word);
            }
        }
    }

    public static class sortComparator extends WritableComparator {
        protected sortComparator() {
            super(IntWritable.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            IntWritable key1 = (IntWritable) w1;
            IntWritable key2 = (IntWritable) w2;
            return -1 * key1.compareTo(key2);
        }
    }


    public static void main(String[] args) throws Exception { Configuration conf = new Configuration();
        //----------------- JOB 1 -------------------

        Job job1 = Job.getInstance(conf, "word count");
        job1.setJarByClass(Profile3.class);
        job1.setNumReduceTasks(27);


        job1.setMapperClass(TokenizerMapper1.class);
        job1.setCombinerClass(IntSumReducer1.class);
        job1.setReducerClass(IntSumReducer1.class);
        job1.setPartitionerClass(WCPartitioner1.class);


        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path("intermediate_output"));
        job1.waitForCompletion(true);


        //----------------- JOB 2 -------------------

        Job job2 = Job.getInstance(conf, "word count");
        job2.setJarByClass(Profile3.class);
        job2.setNumReduceTasks(28);


        job2.setMapperClass(TokenizerMapper2.class);
        job2.setCombinerClass(IntSumReducer2.class);
        job2.setReducerClass(IntSumReducer2.class);
        job2.setPartitionerClass(WCPartitioner2.class);

        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        job2.setSortComparatorClass(sortComparator.class);


        FileInputFormat.addInputPath(job2, new Path("intermediate_output"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
