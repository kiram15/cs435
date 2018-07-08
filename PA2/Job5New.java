import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.TreeMap;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

public class Job5New {

    /*
    MAPPER FOR OG DATASET
    Input: (dummy key, title<====>docID<====>text.text )
    Functionality: Split on periods, get sentences
    Output: (DocID, {A, eachSentence, lineNumber})
    */

    public static class Job5Mapper1 extends Mapper<Object, Text, IntWritable, Text> {
        IntWritable documentID = new IntWritable();
        Text fullKey = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (value.toString().contains("<====>")) {
                String docID = value.toString().split("<====>")[1];
                documentID.set(Integer.parseInt(docID));
                //if the article has any text in it
                if (value.toString().split("<====>").length > 2) {
                    String[] fullList = value.toString().split("<====>")[2].split("\\.");
                    //go through every sentence
                    for (int i = 0; i < fullList.length; i++) {
                        String full = "A" + "\t!\t!\t" + fullList[i] + "\t!\t!\t" + i;
                        fullKey.set(full);
                        context.write(documentID, fullKey);
                    }
                }
            }
        }
    }

    /*
    MAPPER FOR INTERMEDIATE_OUTPUT4
    Input: (DocID, {unigram, TFvalue, TF-IDFvalue})
    Functionality: Associate unigram w/ tfIDF
    Output: (DocID, {value})
    */

    public static class Job5Mapper2 extends Mapper<Object, Text, IntWritable, Text> {
        IntWritable documentID = new IntWritable();
        Text fullKey = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] input = value.toString().split("\t");
            documentID.set(Integer.parseInt(input[0]));

            String full = "B" + "\t!\t!\t" + input[1] + "\t!\t!\t" + input[3];
            fullKey.set(full);
            context.write(documentID, fullKey);
        }
    }

    public static class Job5Partitioner extends Partitioner<IntWritable, Text> {
        @Override
        public int getPartition(IntWritable docID, Text value, int numReduceTasks) {
            //partition on the natural key (DocID)
            return Math.abs(docID.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

    /*
    REDUCER
    Input: (DocID, {some sentences mixed with (word, tfIDF)})
    Functionality: Make a hashmap of all the ones with
    Output: (DocID, {Top 3 sentences})
 	*/

    public static class Job5Reducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        Text val = new Text();
        String joinType = null;
        //TreeMap<Double, String> allSentences = new TreeMap<Double, String>();

        public void setup(Context context) {
            joinType = context.getConfiguration().get("join.type");
        }

        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            TreeMap<String, Double> wordTfTree = new TreeMap<String, Double>();
            ArrayList<String> sentences = new ArrayList<>();

            for (Text value : values) {
                String[] input = value.toString().split("\t!\t!\t");
                if (input[0].equals("A")) {
                    //A \t!\t!\t sentence \t!\t!\t line number
                    String s = input[1] + "\t!\t!\t" + input[2];
                    sentences.add(s);
                } else if (input[0].equals("B")) {
                    //B \t!\t!\t unigram \t!\t!\t tfIDF
                    wordTfTree.put(input[1], Double.parseDouble(input[2]));
                }
            }

            TreeMap<Double, String> allSentences = new TreeMap<Double, String>();
            for (String sentence : sentences) {
                TreeMap<Double, String> jobMapperTree = new TreeMap<Double, String>();
                String[] input = sentence.split("\t!\t!\t");
                StringTokenizer itr = new StringTokenizer(input[0]);
                int lineNum = Integer.parseInt(input[1]);
                while (itr.hasMoreTokens()) {
                    String curr = itr.nextToken();
                    curr = curr.replaceAll("[^A-Za-z0-9 ]", "").toLowerCase();
                    if (!curr.trim().equals("") && !jobMapperTree.containsValue(curr)) {
                        //look through the tree using the unigram for the value
                        Double tfIDF = wordTfTree.get(curr);
                        //puts unigram and associated tfIDF in tree
                        jobMapperTree.put(tfIDF, curr);

                        if (jobMapperTree.size() > 5) {
                            jobMapperTree.remove(jobMapperTree.firstKey());
                        }
                    }
                }
                //add all top 5 values together for every sentence
                double tfIDF = 0.0;
                for (Double d : jobMapperTree.keySet()) {
                    tfIDF = d + tfIDF;
                }
                //({TFIDF}, line number  - sentence)
                String s = lineNum + "\t!\t!\t" + sentence;
                allSentences.put(tfIDF, s);

                if (allSentences.size() > 3) {
                    allSentences.remove(allSentences.firstKey());
                }
            }

            //now make sure they are all in line order
            TreeMap<Integer, String> sortedNew = new TreeMap<Integer, String>();
            for (String s : allSentences.descendingMap().values()) {
                String[] input = s.split("\t!\t!\t");
                sortedNew.put(Integer.parseInt(input[0]), input[1]);
            }

            String finalSentences = "";

            //concatenate all 3 sentences
            for (String s : sortedNew.values()) {

                finalSentences += "\n" + s;
            }

            val.set(finalSentences);
            context.write(key, val);
            allSentences.clear();
        }
    }
}

