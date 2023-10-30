/**
 * Bespin: reference implementations of "big data" algorithms
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package coursework.InvertedIndexing;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.fd.Object2IntFrequencyDistribution;
import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;
import tl.lin.data.pair.*;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class BuildInvertedIndexCompressed extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(BuildInvertedIndexCompressed.class);

    private static final class MyMapper extends Mapper<LongWritable, Text, PairOfStringInt, IntWritable> {
        private static final PairOfStringInt WORD = new PairOfStringInt();
        private static final IntWritable VALUE = new IntWritable();
        private static final Object2IntFrequencyDistribution<String> COUNTS =
                new Object2IntFrequencyDistributionEntry<>();

        @Override
        public void map(LongWritable docno, Text doc, Context context)
                throws IOException, InterruptedException {
            List<String> tokens = Tokenizer.tokenize(doc.toString());

            // Build a histogram of the terms.
            COUNTS.clear();
            for (String token : tokens) {
                COUNTS.increment(token);
            }

            // Emit postings.
            for (PairOfObjectInt<String> e : COUNTS) {
                WORD.set(e.getLeftElement(), (int) docno.get());
                VALUE.set(e.getRightElement());
                context.write(WORD, VALUE);
            }
        }
    }

    private static final class MyReducer extends
            Reducer<PairOfStringInt, IntWritable, Text, BytesWritable> {
        private static final Text WORD = new Text();
        // writes compressed VInt to a byte array in memory
        private ByteArrayOutputStream byteBuffer = new ByteArrayOutputStream();
        // Wrap around `ByteArrayOutputStream` and write VInt in a binary format it
        private DataOutputStream dataBuffer = new DataOutputStream(byteBuffer);
        private String prevWord = "";
        private int prevDocNo = 0;
        // document frequency: the number of documents that contain the term
        private int df = 0;

        @Override
        public void reduce(PairOfStringInt key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            Iterator<IntWritable> iter = values.iterator();
            String curWord = key.getLeftElement();
            int curDocno = key.getRightElement();

            if (prevWord.length() > 0 && !prevWord.equals(curWord)){
                // create new output stream in which written df ++ [gap ++ tf]
                dataBuffer.flush(); // Pushes all VInts out to bytesBuffer
                ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
                DataOutputStream dataOutput = new DataOutputStream(byteOutput);
                WritableUtils.writeVInt(dataOutput, df);
                dataOutput.write(byteBuffer.toByteArray());
                dataOutput.flush(); // Pushes all VInts out to bytesOutput

                // write the current posting list to context and clear it later
                WORD.set(prevWord);
                context.write(WORD, new BytesWritable(byteOutput.toByteArray()));

                // reset states for new (word, docno)
                byteBuffer.reset();
                df = 0;
                prevDocNo = 0;
            }
            prevWord = curWord;
            df++;

            int gap = curDocno - prevDocNo;
            int sum = 0;
            // Aggregate the term frequency for current docno
            while (iter.hasNext()) {
                sum += iter.next().get();
            }
            // add the (gap, termFrequency) to the posting list
            WritableUtils.writeVInt(dataBuffer, gap);
            WritableUtils.writeVInt(dataBuffer, sum);
            prevDocNo = curDocno;

        }

        @Override
        public void cleanup (Context context) throws IOException, InterruptedException {
            // create new output stream in which written df ++ [gap ++ tf]
            dataBuffer.flush(); // Pushes all VInts out to bytesBuffer
            ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
            DataOutputStream dataOutput = new DataOutputStream(byteOutput);
            WritableUtils.writeVInt(dataOutput, df);
            dataOutput.write(byteBuffer.toByteArray());
            dataOutput.flush(); // Pushes all VInts out to bytesOutput
            // write the last posting list to context
            WORD.set(prevWord);
            context.write(WORD, new BytesWritable(byteOutput.toByteArray()));
        }
    }

    private static final class MyPartitioner extends Partitioner<PairOfStringInt, IntWritable> {
        @Override
        public int getPartition(PairOfStringInt key, IntWritable value, int numReduceTasks) {
            return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

    private BuildInvertedIndexCompressed() {}

    private static final class Args {
        @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
        String input;

        @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
        String output;

        @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
        int numReducers = 1;
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

        LOG.info("Tool: " + BuildInvertedIndexCompressed.class.getSimpleName());
        LOG.info(" - input path: " + args.input);
        LOG.info(" - output path: " + args.output);
        LOG.info(" - num reducers: " + args.numReducers);

        Job job = Job.getInstance(getConf());
        job.setJobName(BuildInvertedIndexCompressed.class.getSimpleName());
        job.setJarByClass(BuildInvertedIndexCompressed.class);

        job.setNumReduceTasks(args.numReducers);

        FileInputFormat.setInputPaths(job, new Path(args.input));
        FileOutputFormat.setOutputPath(job, new Path(args.output));

        job.setMapOutputKeyClass(PairOfStringInt.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        job.setOutputFormatClass(MapFileOutputFormat.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setPartitionerClass(MyPartitioner.class);

        // Delete the output directory if it exists already.
        Path outputDir = new Path(args.output);
        FileSystem.get(getConf()).delete(outputDir, true);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
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
        ToolRunner.run(new BuildInvertedIndexCompressed(), args);
    }
}
