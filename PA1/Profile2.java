import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Profile2 {
    private static class idWordKey implements WritableComparable<idWordKey> {
        Text docID = new Text();
        Text word = new Text();

        public idWordKey() {
        }

        public Text getDocID() {
            return docID;
        }

        public Text getWord() {
            return word;
        }

        //Text constructor
        public idWordKey(Text docID, Text word) {
            this.docID = docID;
            this.word = word;
        }

        public void write(DataOutput out) throws IOException {
            this.docID.write(out);
            this.word.write(out);
        }
        public void readFields(DataInput in) throws IOException {
            this.docID.readFields(in);
            this.word.readFields(in);
        }

        public int compareTo(idWordKey pop) {
            if (pop == null)
                return 0;
            int intcnt = this.docID.compareTo(pop.docID);
            return intcnt == 0 ? word.compareTo(pop.word) : intcnt;
        }
        @Override
        public String toString() {
            return docID + "\t" + word;
        }
    }

    private static class idFreqKey implements WritableComparable<idFreqKey> {
        Text docID = new Text();
        IntWritable freq = new IntWritable();

        public idFreqKey() {
        }

        public Text getDocID() {
            return docID;
        }

        public IntWritable getFreq() {
            return freq;
        }

        public idFreqKey(Text docID, IntWritable freq) {
            this.docID = docID;
            this.freq = freq;
        }

        public void write(DataOutput out) throws IOException {
            this.docID.write(out);
            this.freq.write(out);
        }
        public void readFields(DataInput in) throws IOException {
            this.docID.readFields(in);
            this.freq.readFields(in);
        }

        public int compareTo(idFreqKey pop) {
            if (pop == null)
                return 0;
            int intcnt = docID.compareTo(pop.docID);
            return intcnt == 0 ? freq.compareTo(pop.freq) : intcnt;
        }
        @Override
        public String toString() {
            return docID + "\t" + freq;
        }
    }

    public static class TokenizerMapper1 extends Mapper<Object, Text, idWordKey, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        Text ID = new Text();
        Text word = new Text();
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            if (value.toString().contains("<====>")) {
                //Isolate Document ID
                String docID = value.toString().split("<====>")[1];
                ID.set(docID);
                StringTokenizer itr = new StringTokenizer(value.toString().split("<====>")[2]);

                //Allocate count 1 for each word
                while (itr.hasMoreTokens()) {
                    String curr = itr.nextToken();
                    //Get rid of everything
                    curr = curr.replaceAll("[^A-Za-z0-9 ]", "").toLowerCase();
                    if (!curr.equals("")) {
                        word.set(curr);
                        idWordKey fullKey = new idWordKey(ID, word);
                        context.write(fullKey, one);
                    }
                }
            }
        }
    }

    public static class IntSumReducer1 extends Reducer<idWordKey, IntWritable, idWordKey, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(idWordKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            //sum up every value for each word
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class TokenizerMapper2 extends Mapper<LongWritable, Text, idFreqKey, Text> {
        Text docID = new Text();
        Text unigram = new Text();
        IntWritable frequency = new IntWritable();
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t"); //Doc ID - unigram - frequency
            docID.set(line[0]);
            unigram.set(line[1]);
            frequency.set(Integer.parseInt(line[2]));

            //sorts by DocId and then frequency
            idFreqKey fullKey = new idFreqKey(docID, frequency);
            context.write(fullKey, unigram);
        }
    }

    public static class IntSumReducer2 extends Reducer<idFreqKey, Text, idFreqKey, Text> {
        private Text unigram = new Text();

        public void reduce(idFreqKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //for all of the unigrams
            for (Text value : values) {
                String s = value + "\t" + key.getFreq().toString();
                unigram.set(value);
                context.write(key, unigram);
            }
        }
    }

    public static class idWordComparator extends WritableComparator {
        protected idWordComparator() {
            super(idWordKey.class, true);
        }
        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            idWordKey k1 = (idWordKey)w1;
            idWordKey k2 = (idWordKey)w2;

            int result = k1.getDocID().compareTo(k2.getDocID());
            if(0 == result) {
                result = -1* k1.getWord().compareTo(k2.getWord());
            }
            return result;
        }
    }

    public static class idFreqComparator extends WritableComparator {
        protected idFreqComparator() {
            super(idFreqKey.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            idFreqKey k1 = (idFreqKey) w1;
            idFreqKey k2 = (idFreqKey) w2;

            int result = k1.getDocID().compareTo(k2.getDocID());
            if (0 == result) {
                result = -1 * k1.getFreq().compareTo(k2.getFreq());
            }
            return result;
        }
    }

    public static class WCPartitioner1 extends Partitioner<idWordKey, IntWritable> {
        @Override
        public int getPartition(idWordKey compositeKey, IntWritable value, int numReduceTasks) {
            //partition on the natural key (DocID)
            return Math.abs(compositeKey.getDocID().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

    public static class WCPartitioner2 extends Partitioner<idFreqKey, Text> {
        @Override
        public int getPartition(idFreqKey compositeKey, Text value, int numReduceTasks) {
            //partition on the natural key (DocID)
            return Math.abs(compositeKey.getDocID().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "word count");
        job1.setJarByClass(Profile2.class);
        job1.setNumReduceTasks(27);

        job1.setMapperClass(TokenizerMapper1.class);
        job1.setCombinerClass(IntSumReducer1.class);
        job1.setReducerClass(IntSumReducer1.class);
        job1.setSortComparatorClass(idWordComparator.class);
        job1.setPartitionerClass(WCPartitioner1.class);


        job1.setOutputKeyClass(idWordKey.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path("intermediate_output"));
        job1.waitForCompletion(true);


        // -- JOB 2 --

        Job job2 = Job.getInstance(conf, "word count2");
        job2.setJarByClass(Profile2.class);
        job2.setNumReduceTasks(27);

        job2.setMapperClass(TokenizerMapper2.class);
        job2.setCombinerClass(IntSumReducer2.class);
        job2.setReducerClass(IntSumReducer2.class);
        job2.setSortComparatorClass(idFreqComparator.class);
        job2.setPartitionerClass(WCPartitioner2.class);

        job2.setOutputKeyClass(idFreqKey.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path("intermediate_output"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
