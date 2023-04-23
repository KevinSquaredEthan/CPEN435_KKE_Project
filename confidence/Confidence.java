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
       extends Mapper<Object, Text, Text, IntWritable>{
   
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    // for caseSensitive
    private boolean caseSensitive;
    private Configuration conf;
    private BufferedReader fis;

    @Override
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      ArrayList<String> A = new ArrayList<String>(); // String not Text, as sort works with strings
      HashMap<String, int> single_word_map;
      String single_string;
      int single_int;
      String first_in_pair;
      String rest_in_pair;
      String second_in_pair;
      int combo_int;
      
      String file = value.toString();
      StringTokenizer itr = new StringTokenizer(file,"\n"); // iterate through each line
      // First pass to store all single words values
      while (itr.hasMoreTokens()) { 
	String line = itr.nextToken();
	if(!line.contains(":")) {
	   StringTokenizer itr2 = new StringTokenizer(line, "\t"); // tab in between key and value
	   single_word_set.put(itr2.nextToken(),Integer.parseInt(itr2.nextToken())); // store single word
	}
      }
      // Second pass to create combination keys + add associated values
      StringTokenizer itr3 = new StringTokenizer(file,"\n");
      while(itr3.hasMoreTokens()){
      	String line = itr.nextToken();
	StringTokenizer itr4 = new StringTokenizer(line,":");
	if(itr4.hasMoreTokens()){
		String word_one = itr4.nextToken();
		// check if it's a pair
		if(itr4.hasMoreTokens()){
			// parse the combination pair
			IntWriteable word_one_count = new IntWriteable(single_word_map.get(word_one));
			String word_two = itr4.nextToken("\t");
			IntWriteable word_two_count = new IntWriteable(single_word_map.get(word_two));
			IntWriteable combination = new IntWriteable(Integer.parseInt(itr4.nextToken()));
			word.set(word_one+":"+word_two);
			// write as (combination; word one count, word two count, combination count)
			// 		(key; value1, value2, value3)
			context.write(word,word_one_count,word_two_count,combination);
		}
	}
      }
      
		    
      }
    }
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
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private DoubleWritable result = new DoubleWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int word_one_num = val.get();
      int word_two_num = val.get();
      int combination_num = val.get();
      result.set(((double)combination_num) / ((double)word_one_num));
      context.write(key, result);
      StringTokenizer itr = new StringTokenizer(key.toString(),":");
      String temp = itr.nextToken();
      String reverse_key = itr.nextToken()+":"+temp;
      result.set(((double)combination_num) / ((double)word_two_num));
      context.write(reverse_key.toText(),result);
    }
  } // TODO maybe need this but need to reduce multiple keys

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
    job2.setMapperClass(MapTwo.class);
    job2.setReducerClass(ReduceTwo.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job2, new Path(otherArgs.get(2)));
    FileOutputFormat.setOutputPath(job2, new Path(otherArgs.get(3)));
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}
