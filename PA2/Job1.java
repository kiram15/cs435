import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

public class Job1 {
    /*
    MAPPER
	Input: Raw input
	Functionality: Separate info, get docID, create composite key
	Output: ({DocID, unigram}, 1)
 	*/

    public static class Job1Mapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        Text docIDUnigram = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (value.toString().contains("<====>")) {
                String docID = value.toString().split("<====>")[1];
                if (value.toString().split("<====>").length > 2) {
                    String[] fullList = value.toString().split("<====>")[2].split("\\.");

                    //get the entire article after the info
                    for (int i = 0; i < fullList.length; i++) {
                        if (!(fullList[i].equals("")) || !(fullList[i].equals(" "))) {
                            StringTokenizer itr = new StringTokenizer(fullList[i]);
                            while (itr.hasMoreTokens()) {
                                String curr = itr.nextToken();
                                curr = curr.replaceAll("[^A-Za-z0-9 ]", "").toLowerCase();
                                if (!curr.equals("")) {
                                    String fullKey = docID + "\t" + curr;
                                    docIDUnigram.set(fullKey);
                                    context.write(docIDUnigram, one);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    public static class Job1Partitioner extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text compositeKey, IntWritable value, int numReduceTasks) {
            //partition on the natural key (DocID)
            String docID = compositeKey.toString().split("\t")[0];
            return Math.abs(docID.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

    /*
	REDUCER
	Input: List of {DocID, unigram} matches
	Functionality: concatenate all unigrams from one doc, increment frequency
	Output: ({DocID, unigram}, frequency)
	 */

    public static class Job1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}
