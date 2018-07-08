import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class Job2 {
    /*
	MAPPER
    Input: ({DocID, unigram}, frequency)
    Functionality: switch the values?
    Output: (DocID, {unigram, frequency})
 	*/

    public static class Job2Mapper extends Mapper<Object, Text, IntWritable, Text> {
        Text unigramFrequency = new Text();
        IntWritable docID = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //format of input file: docID \t unigram \t frequency
            String[] input = value.toString().split("\t");
            docID.set(Integer.parseInt(input[0]));
            String fullKey = input[1] + "\t" + input[2];
            unigramFrequency.set(fullKey);
            context.write(docID, unigramFrequency);
        }
    }

    public static class Job2Partitioner extends Partitioner<IntWritable, Text> {
        @Override
        public int getPartition(IntWritable docID, Text value, int numReduceTasks) {
            //partition on the natural key (DocID)
            return Math.abs(docID.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

	/*
	REDUCER
    Input: List of {unigram, frequency} values for each DocID
    Functionality: Find the max raw frequency of any term k in article j, then calculate the TF
    Output: (DocID, {unigram, frequency, TFvalue})

	 */

    public static class Job2Reducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        Text fullKey = new Text();

        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            context.getCounter(Driver.CountersClass.UpdateCount.N).increment(1);

            ArrayList<Text> cache = new ArrayList<Text>();

            for (Text val : values) {
                Text writable = val;
                cache.add(new Text(writable));
            }

            //Find the max raw frequency of any term k in article j
            double max = -1.0;
            for (int i = 0; i < cache.size(); i++) {
                //Text s = cache.get(i);
                int number = Integer.parseInt(cache.get(i).toString().split("\t")[1]);
                if (number > max) {
                    max = number;
                }
            }

            //TF of unigram i in doc j = 0.5 + 0.5(frequency of word i in j / max raw frequency of any term k in article j)

            //Input: docID \t unigram \t frequency
            //Output: (DocID, {unigram, frequency, TFvalue})
            for (int i = 0; i < cache.size(); i++) {
                String[] info = cache.get(i).toString().split("\t");
                Double freq = Double.parseDouble(info[1]);
                double tf = 0.5 + 0.5 * (freq / max);
                String full = info[0] + "\t" + info[1] + "\t" + tf;
                fullKey.set(full);
                context.write(key, fullKey);
            }
        }
    }
}
