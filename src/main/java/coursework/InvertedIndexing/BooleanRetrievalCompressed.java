package coursework.InvertedIndexing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.pair.PairOfInts;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.DataInputStream;
import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;
import java.util.Arrays;
import java.util.Comparator;

public class BooleanRetrievalCompressed extends Configured implements Tool {
    private List<MapFile.Reader> indices = new ArrayList<>();
    private int numReducers;
    private FSDataInputStream collection;
    private Stack<Set<Integer>> stack;

    private BooleanRetrievalCompressed() {}

    private void initialize(String indexPath, String collectionPath, FileSystem fs) throws IOException {
        PathFilter pathFilter = new PathFilter() {
            @Override
            public boolean accept (Path path) {
                return !path.getName().contains("SUCCESS") && path.getName().startsWith("part-r-");
            }
        };
        // Need to sort files by pathname in lexicographical order
        FileStatus[] statuses = fs.listStatus(new Path(indexPath), pathFilter);
        Arrays.sort(statuses, new Comparator<FileStatus>() {
            @Override
            public int compare(FileStatus o1, FileStatus o2) {
                return o1.getPath().toString().compareTo(o2.getPath().toString());
            }
        });
        for (FileStatus status: statuses){
            MapFile.Reader index = new MapFile.Reader(status.getPath(), fs.getConf());
            indices.add(index);
            System.out.println("Add index " + status.getPath());
        }
        numReducers = indices.size();
        System.out.println("The number of reducers detected: " + numReducers);

        collection = fs.open(new Path(collectionPath));
        stack = new Stack<>();
    }

    private void runQuery(String q) throws IOException {
        String[] terms = q.split("\\s+");

        for (String t : terms) {
            if (t.equals("AND")) {
                performAND();
            } else if (t.equals("OR")) {
                performOR();
            } else {
                pushTerm(t);
            }
        }

        Set<Integer> set = stack.pop();

        for (Integer i : set) {
            String line = fetchLine(i);
            System.out.println(i + "\t" + line);
        }
    }

    private void pushTerm(String term) throws IOException {
        stack.push(fetchDocumentSet(term));
    }

    private void performAND() {
        Set<Integer> s1 = stack.pop();
        Set<Integer> s2 = stack.pop();

        Set<Integer> sn = new TreeSet<>();

        for (int n : s1) {
            if (s2.contains(n)) {
                sn.add(n);
            }
        }

        stack.push(sn);
    }

    private void performOR() {
        Set<Integer> s1 = stack.pop();
        Set<Integer> s2 = stack.pop();

        Set<Integer> sn = new TreeSet<>();

        for (int n : s1) {
            sn.add(n);
        }

        for (int n : s2) {
            sn.add(n);
        }

        stack.push(sn);
    }

    private Set<Integer> fetchDocumentSet(String term) throws IOException {
        Set<Integer> set = new TreeSet<>();

        for (PairOfInts pair : fetchPostings(term)) {
            set.add(pair.getLeftElement());
        }

        return set;
    }

    private ArrayListWritable<PairOfInts> fetchPostings(String term) throws IOException {
        Text key = new Text();
        key.set(term);
        BytesWritable value = new BytesWritable();
        MapFile.Reader index = indices.get((term.hashCode() & Integer.MAX_VALUE) % numReducers);
        if (index.get(key, value) == null)
            System.out.println("The key " + term + " is not found in index.");
//            throw new IOException("The key " + term + " is not found in index.");
        ByteArrayInputStream byteStream = new ByteArrayInputStream(value.getBytes());
        DataInputStream dataStream = new DataInputStream(byteStream);

        if(dataStream.available() <= 0) {
            System.out.println("Expected data not present in stream for term: " + term);
//            throw new IOException("Expected data not present in stream for term: " + term);
        }
        
        ArrayListWritable<PairOfInts> postings = new ArrayListWritable<>();
        int df = WritableUtils.readVInt(dataStream);
        int docno = 0;
        for (int i = 0; i < df; i++){
            int gap = WritableUtils.readVInt(dataStream);
            int termFrequency = WritableUtils.readVInt(dataStream);
            docno += gap;
            postings.add(new PairOfInts(docno, termFrequency));
        }
        assert byteStream.available() == 0: "df not consistent with len(postings)";

        // return the posting list
        return postings;
    }

    public String fetchLine(long offset) throws IOException {
        collection.seek(offset);
        BufferedReader reader = new BufferedReader(new InputStreamReader(collection));

        String d = reader.readLine();
        return d.length() > 80 ? d.substring(0, 80) + "..." : d;
    }

    private static final class Args {
        @Option(name = "-index", metaVar = "[path]", required = true, usage = "index path")
        String index;

        @Option(name = "-collection", metaVar = "[path]", required = true, usage = "collection path")
        String collection;

        @Option(name = "-query", metaVar = "[term]", required = true, usage = "query")
        String query;
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

        if (args.collection.endsWith(".gz")) {
            System.out.println("gzipped collection is not seekable: use compressed version!");
            return -1;
        }

        FileSystem fs = FileSystem.get(new Configuration());

        initialize(args.index, args.collection, fs);

        System.out.println("Query: " + args.query);
        long startTime = System.currentTimeMillis();
        runQuery(args.query);
        System.out.println("\nquery completed in " + (System.currentTimeMillis() - startTime) + "ms");

        return 1;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     *
     * @param args command-line arguments
     * @throws Exception if tool encounters an exception
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new BooleanRetrievalCompressed(), args);
    }
}
