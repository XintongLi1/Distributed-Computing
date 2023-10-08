package coursework.MapReduce;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.pair.PairOfStrings;
import tl.lin.data.pair.PairOfFloatInt;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

public class PairsPMI extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(PairsPMI.class);
    private static final int WORD_LIMIT = 40;

    public static final class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        // Reuse objects to save overhead of object creation.
        private static final IntWritable ONE = new IntWritable(1);
        private static final Text WORD = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            List<String> tokens = Tokenizer.tokenize(value.toString());
            HashSet<String> uniqueWords = new HashSet<>();
            // Count the total number of lines of the file
            WORD.set("*");
            context.write(WORD, ONE);
            // Count the number of lines in which the word occur
            for (int i = 0; i < Math.min(tokens.size(), WORD_LIMIT); i++){
                uniqueWords.add(tokens.get(i));
            }
            for (String word : uniqueWords) {
                WORD.set(word);
                context.write(WORD, ONE);
            }
        }
    }


    // Reducer: sums up all the counts.
    public static final class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        // Reuse objects.
        private static final IntWritable SUM = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            // Sum up values.
            Iterator<IntWritable> iter = values.iterator();
            int sum = 0;
            while (iter.hasNext()) {
                sum += iter.next().get();
            }
            SUM.set(sum);
            context.write(key, SUM);
        }
    }

    private static final class MyMapper extends Mapper<LongWritable, Text, PairOfStrings, FloatWritable> {
        private static final FloatWritable ONE = new FloatWritable(1);
        private static final PairOfStrings PAIR = new PairOfStrings();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            List<String> tokens = Tokenizer.tokenize(value.toString());
            if (tokens.size() < 2) return;
            HashSet<String> uniqueWords = new HashSet<>();
            // Get the unique words on the same line
            for (int i = 0; i < Math.min(tokens.size(), WORD_LIMIT); i++){
                uniqueWords.add(tokens.get(i));
            }
            for (String word1 : uniqueWords) {
                for (String word2: uniqueWords){
                    if (!word1.equals(word2)){
                        PAIR.set(word1, word2);
                        context.write(PAIR, ONE);
                    }
                }
            }
        }
    }

    private static final class MyCombiner extends
            Reducer<PairOfStrings, FloatWritable, PairOfStrings, FloatWritable> {
        private static final FloatWritable SUM = new FloatWritable();

        @Override
        public void reduce(PairOfStrings key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            Iterator<FloatWritable> iter = values.iterator();
            while (iter.hasNext()) {
                sum += iter.next().get();
            }
            SUM.set(sum);
            context.write(key, SUM);
        }
    }

    private static final class MyReducer extends
            Reducer<PairOfStrings, FloatWritable, PairOfStrings, PairOfFloatInt> {
        private static final PairOfFloatInt VALUE = new PairOfFloatInt();
        private static int threshold;
        private static HashMap<String, Integer> wordCount = new HashMap<>();

        @Override
        public void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            threshold = conf.getInt("threshold", 10);
            Path input_path = new Path(conf.get("wordCountPath") + "/part-r-00000");
            FileSystem fileSystem = FileSystem.get(conf);
            try (BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(input_path)))){
                String line;
                while ((line = br.readLine()) != null) {
                    // Split the line into two parts: string and integer
                    String[] parts = line.split("\\s+");
                    if (parts.length == 2) {
                        String key = parts[0];
                        int value = Integer.parseInt(parts[1]);
                        wordCount.put(key, value);
                    } else {
                        LOG.info("WordCount File has invalid entry: " + line);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void reduce(PairOfStrings key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            int numLine = wordCount.get("*");

            Iterator<FloatWritable> iter = values.iterator();
            while (iter.hasNext()) {
                sum += iter.next().get();
            }
            if (sum >= threshold){
                int word1Occurrence = wordCount.get(key.getLeftElement());
                int word2Occurrence = wordCount.get(key.getRightElement());
                float PMI = (float) Math.log10(sum * 1.0f * numLine / (word1Occurrence *  word2Occurrence));
                VALUE.set(PMI, sum);
                context.write(key, VALUE);
            }
        }
    }

    private static final class MyPartitioner extends Partitioner<PairOfStrings, FloatWritable> {
        @Override
        public int getPartition(PairOfStrings key, FloatWritable value, int numReduceTasks) {
            return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

    /**
     * Creates an instance of this tool.
     */
    private PairsPMI() {}

    private static final class Args {
        @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
        String input;

        @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
        String output;

        @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
        int numReducers = 1;

        @Option(name = "-threshold", metaVar = "[num]", usage = "threshold of co-occurrence")
        int threshold = 10;

        @Option(name = "-textOutput", usage = "use TextOutputFormat (otherwise, SequenceFileOutputFormat)")
        boolean textOutput = true;
    }

    /**
     * Runs this tool.
     */
    @Override
    public int run(String[] argv) throws Exception {
        final Args args = new Args();
        CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

        try {
            parser.parseArgument(argv);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
            return -1;
        }

        /*
            The first job: compute the word count
                - the number of lines that contain a word
         */

        String wordCountTempPath = args.output + "-000111000-temp";

        LOG.info("Tool name: " + PairsPMI.class.getSimpleName());
        LOG.info("Start the first job");
        LOG.info(" - input path: " + args.input);
        LOG.info(" - output path: " + wordCountTempPath);
        LOG.info(" - threshold: " + args.threshold);
        LOG.info(" - num reducers: " + args.numReducers);
        LOG.info(" - text output: " + args.textOutput);


        Configuration conf = getConf();
        conf.setInt("threshold", args.threshold);
        conf.set("wordCountPath", wordCountTempPath);

        Job job1 = Job.getInstance(conf);
        job1.setJobName(PairsPMI.class.getSimpleName() + "-Part1");
        job1.setJarByClass(PairsPMI.class);

        // Setting these MapReduce job parameters for running on the Datasci cluster
        job1.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
        job1.getConfiguration().set("mapreduce.map.memory.mb", "3072");
        job1.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
        job1.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
        job1.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

        job1.setNumReduceTasks(1); // no partition for this job

        FileInputFormat.setInputPaths(job1, new Path(args.input));
        FileOutputFormat.setOutputPath(job1, new Path(wordCountTempPath));

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        job1.setMapperClass(WordCountMapper.class);
        job1.setCombinerClass(WordCountReducer.class);
        job1.setReducerClass(WordCountReducer.class);

        // Delete the output directory if it exists already.
        Path tempDir = new Path(wordCountTempPath);
        FileSystem.get(getConf()).delete(tempDir, true);

        long startTime = System.currentTimeMillis();
        job1.waitForCompletion(true);
        LOG.info("The first job finished");


        /*
            The second Job: Compute pointwise mutual information
         */
        LOG.info("Start the second job");
        LOG.info(" - input path: " + args.input);
        LOG.info(" - output path: " + args.output);
        LOG.info(" - threshold: " + args.threshold);
        LOG.info(" - num reducers: " + args.numReducers);
        LOG.info(" - text output: " + args.textOutput);

        Job job2 = Job.getInstance(conf);
        job2.setJobName(PairsPMI.class.getSimpleName() + "-Part2");
        job2.setJarByClass(PairsPMI.class);

        // Setting these MapReduce job parameters for running on the Datasci cluster
        job2.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
        job2.getConfiguration().set("mapreduce.map.memory.mb", "3072");
        job2.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
        job2.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
        job2.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

        job2.setNumReduceTasks(args.numReducers);

        FileInputFormat.setInputPaths(job2, new Path(args.input));
        FileOutputFormat.setOutputPath(job2, new Path(args.output));

        job2.setMapOutputKeyClass(PairOfStrings.class);
        job2.setMapOutputValueClass(FloatWritable.class);
        job2.setOutputKeyClass(PairOfStrings.class);
        job2.setOutputValueClass(PairOfFloatInt.class);
        if (args.textOutput) {
            job2.setOutputFormatClass(TextOutputFormat.class);
        } else {
            job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        }

        job2.setMapperClass(MyMapper.class);
        job2.setCombinerClass(MyCombiner.class);
        job2.setReducerClass(MyReducer.class);
        job2.setPartitionerClass(MyPartitioner.class);


        // Delete the output directory if it exists already.
        Path outputDir = new Path(args.output);
        FileSystem.get(getConf()).delete(outputDir, true);

        job2.waitForCompletion(true);

        System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        return 0;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     *
     * @param args command-line arguments
     * @throws Exception if tool encounters an exception
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new PairsPMI(), args);
    }
}

