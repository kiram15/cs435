import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.IOException;

public class Job4 {
    /*
    MAPPER
    Input: (unigram, {DocID, TFvalue, nj})
    Functionality: Look up how to get N (total # of unique documents) – use lec3 references,
    then calculate IDF, then calculate TFi*IDFi
    Output: (DocID, {unigram, TFvalue, TF-IDFvalue})
 	*/

    public static class Job4Mapper extends Mapper<Object, Text, IntWritable, Text> {
        IntWritable docID = new IntWritable();
        Text fullKey = new Text();

        private long someCount;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            super.setup(context);
            this.someCount  = context.getConfiguration().getLong(Driver.CountersClass.UpdateCount.N.name(), 0);
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //format of input file: unigram \t docID \t TFvalue \t nj
            String[] input = value.toString().split("\t");

            double IDF = Math.log10(someCount / Double.parseDouble(input[3]));
            double TFIDF = Double.parseDouble(input[2]) * IDF;

            String full = input[0] + "\t" + input[2] + "\t" + TFIDF; //docID, TFvalue, TF-IDF
            docID.set(Integer.parseInt(input[1]));

            fullKey.set(full);
            context.write(docID, fullKey);
        }
    }

    public static class Job4Partitioner extends Partitioner<IntWritable, Text> {
        @Override
        public int getPartition(IntWritable docID, Text value, int numReduceTasks) {
            //partition on the natural key (DocID)
            return Math.abs(docID.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }
}
