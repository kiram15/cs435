import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class Job3 {

    /*
	MAPPER
    Input: (DocID, {unigram, frequency, TFvalue})
    Functionality: switch the values?
    Output: (unigram, {DocID, TFvalue})
 	*/

    public static class Job3Mapper extends Mapper<Object, Text, Text, Text> {
        Text unigram = new Text();
        Text fullKey = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //format of input file: docID \t unigram \t frequency \t TFvalue
            String[] input = value.toString().split("\t");
            unigram.set(input[1]);
            String full = input[0] + "\t" + input[3]; //docID and TFvalue
            fullKey.set(full);
            context.write(unigram, fullKey);
        }
    }

    public static class Job3Partitioner extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text unigram, Text value, int numReduceTasks) {
            //partition on the natural key (DocID)
            String docID = value.toString().split("\t")[0];
            return Math.abs(docID.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

	/*
	REDUCER
    Input: list of {DocID, TFvalue} for each unigram
    Functionality: Find total occurrences of the unigram in the whole dataset - nj
    Then look up how to get N (total # of unique documents) – use lec3 references
    Output: (unigram, {DocID, TFvalue, nj})
	 */

    public static class Job3Reducer extends Reducer<Text, Text, Text, Text> {
        Text fullKey = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            ArrayList<Text> cache = new ArrayList<Text>();

            for (Text val : values) {
                Text writable = val;
                cache.add(new Text(writable));
            }

            for (int i = 0; i < cache.size(); i++) {
                String[] info = cache.get(i).toString().split("\t");
                String full = info[0] + "\t" + info[1] + "\t" + cache.size(); //cache.size = nj
                fullKey.set(full);
                context.write(key, fullKey);
            }
        }
    }
}
