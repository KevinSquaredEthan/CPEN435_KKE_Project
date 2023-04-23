import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Collections;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

public class Confidence {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    // for caseSensitive
    private boolean caseSensitive;
    private Set<String> patternsToSkip = new HashSet<String>();
    private Set<String> stopWordsToSkip = new HashSet<String>();
    private Configuration conf;
    private BufferedReader fis;

    @Override
    public void setup(Context context) throws IOException,
		InterruptedException {
	conf = context.getConfiguration();
	caseSensitive = conf.getBoolean("confidence.case.sensitive", true); 
	//not case sensitive
	if (conf.getBoolean("confidence.skip.patterns", false)) {
		URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
		for (URI patternsURI : patternsURIs) {
			Path patternsPath = new Path(patternsURI.getPath());
			String patternsFileName = patternsPath.getName().toString();
                        if(patternsFileName.equals("patterns.txt")) {
			parseSkipFile(patternsFileName); }
			if(patternsFileName.equals("stop_words.txt")) {
			parseStopWordsFile(patternsFileName); }
		}
	}
    }

    private void parseSkipFile(String fileName) {
	try {
		fis = new BufferedReader(new FileReader(fileName));
		String pattern = null;
		while ((pattern = fis.readLine()) != null) {
			patternsToSkip.add(pattern);
		}
	} catch (IOException ioe) {
		System.err.println("Caught exception while parsing the cached file '"
		+ StringUtils.stringifyException(ioe));
	}
     }

    private void parseStopWordsFile(String fileName) {
	try {
		fis = new BufferedReader(new FileReader(fileName));
		String pattern = null;
		while ((pattern = fis.readLine()) != null) {
			stopWordsToSkip.add(pattern);
		}
	} catch (IOException ioe) {
		System.err.println("Caught exception while parsing the cached file '"
		+ StringUtils.stringifyException(ioe));
	}
     }

    
    @Override
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      ArrayList<String> A = new ArrayList<String>(); // String not Text, as sort works with strings
       // A could possibly be a hash set
      // caseSensitive and removes punctuation
      String line = (caseSensitive) ?
	value.toString() : value.toString().toLowerCase();
      for (String pattern : patternsToSkip) {
	line = line.replaceAll(pattern, "");
      }
      StringTokenizer itr = new StringTokenizer(line,"\n"); // iterate through each line
      while (itr.hasMoreTokens()) { 
	StringTokenizer itr2 = new StringTokenizer(itr.nextToken(), " ");
	// this loop will find a list of unique words in a line
	while(itr2.hasMoreTokens()) {
	 String temp = itr2.nextToken();
         if(A.contains(temp) == false && !stopWordsToSkip.contains(temp))
	   // check if it stop word to not add
	   A.add(temp);
	}
      }
      Collections.sort(A); // in ascending order 
      for(int i = 0; i < A.size(); ++i) {
	String wi = A.get(i);
	word.set(wi);
        context.write(word, one); //w[i] emit
	for(int j = i+1; j < A.size(); ++j) {
	  word.set(wi+":"+A.get(j));
	  // TODO account for converse like hello:world and world:hello 
          context.write(word, one); 
	  //emit pair of w[i] and w[j]
	}
      }
    }
  } // end of mapper class

  public static class ConfMapper
       extends Mapper<Text, IntWritable, Text, Text>{
   
    private Text word1 = new Text();
    private Text word2 = new Text();
    // for caseSensitive
    private boolean caseSensitive;
    private Configuration conf;
    private BufferedReader fis;

    @Override
    // key is object as writes to that 
    public void map(Text key, IntWritable value, Context context
                    ) throws IOException, InterruptedException {
      String line = key.toString();
      if(line.contains(":")) { // is a pair
	StringTokenizer itr2 = new StringTokenizer(line, ":");
	String first_pair = itr2.nextToken();
	word1.set(first_pair); // like hello in hello:world 2
	String second_pair = itr2.nextToken();
	// value has number
	word2.set(second_pair+"|"+value.toString());
	context.write(word1,word2);
	// hello:world but also world:hello
	word1.set(second_pair);
	word2.set(first_pair+"|"+value.toString());
	context.write(word1,word2);
      }
      else {
	StringTokenizer itr1 = new StringTokenizer(line, "\t");
	word1.set(itr1.nextToken());
	word2.set(value.toString());
        context.write(word1,word2); 
	// write that to context and cast to string
      }
    } // end of mapper function
  } // end of mapper class

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static class ConfReducer
       extends Reducer<Text,Text,Text,Text> {
    private Text word1 = new Text();
    private Text word2 = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      double numerator = 0;
      double denominator = 0;
      boolean set_score = false;
      for (Text val : values) {
	if(val.toString().contains("|")) {
          
	 set_score = true;
	}
	else {
          denominator = Double.parseDouble(val.toString());
        }
	if(set_score) { // if both have vals for num and den
	 word2.set(String.valueOf(numerator/denominator));
	 // set to conf score
         context.write(key, word2); 
	 set_score = false;
	}
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    GenericOptionsParser optionParser = new GenericOptionsParser(conf,
	args);
    String[] remainingArgs = optionParser.getRemainingArgs();
    /*if ((remainingArgs.length != 2) && (remainingArgs.length != 5)) {
	System.err.println("Usage: confidence <in> <out> [-skip skipPatternFile]");
	System.exit(2);
    }*/ // TODO put this back later
    Job job = Job.getInstance(conf, "confidence");
    job.setJarByClass(Confidence.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    List<String> otherArgs = new ArrayList<String>();
    for (int i=0; i < remainingArgs.length; ++i) {
	if ("-skip".equals(remainingArgs[i])) {
		job.addCacheFile(new Path(remainingArgs[++i]).toUri());
		job.getConfiguration().setBoolean("confidence.skip.patterns", true);
	} else {
		otherArgs.add(remainingArgs[i]);
	}
    }
    FileInputFormat.addInputPath(job, new Path(args[2]));
    FileOutputFormat.setOutputPath(job, new Path(args[3]));
    // running the second job
    job.waitForCompletion(true) ; // first MapReduce job finishes here
    Configuration confTwo = new Configuration();
    Job job2 = Job.getInstance(confTwo, "Conf step two");
    job2.setJarByClass(Confidence.class);
    job2.setMapperClass(ConfMapper.class);
    job2.setReducerClass(ConfReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job2, new Path(otherArgs.get(2)));
    FileOutputFormat.setOutputPath(job2, new Path(otherArgs.get(3)));
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}
