import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Profile1 {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, NullWritable> {
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (value.toString().contains("<====>")) {
                StringTokenizer itr = new StringTokenizer(value.toString().split("<====>")[2]);

                while (itr.hasMoreTokens()) {
                    String curr = itr.nextToken();
                    curr = curr.replaceAll("[^A-Za-z0-9 ]", "").toLowerCase();
                    if (!curr.equals("")) {
                        word.set(curr);
                        context.write(word, NullWritable.get());
                    }
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
        public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    public static class WCPartitioner extends Partitioner <Text, NullWritable> {
        public int getPartition(Text key, NullWritable value, int numReduceTasks) {
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

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(Profile1.class);
        job.setNumReduceTasks(27);

        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setPartitionerClass(WCPartitioner.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
