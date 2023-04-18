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
			parseSkipFile(patternsFileName);
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

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      ArrayList<String> A = new ArrayList<String>(); // String not Text, as sort works with strings
      // caseSensitive and remove stop words
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
         if(A.contains(temp) == false)
	   A.add(temp);
	}
      }
      Collections.sort(A); // in ascending order 
      for(int i = 0; i < A.size(); ++i) {
	String wi = A.get(i);
	word.set(wi);
        context.write(word, one); //w[i] emit
	for(int j = i+1; j < A.size(); ++j) {
	  word.set(wi+" : "+A.get(j));
          context.write(word, one); 
	  //emit pair of w[i] and w[j]
	}
      }
    }
  }

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

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    GenericOptionsParser optionParser = new GenericOptionsParser(conf,
	args);
    String[] remainingArgs = optionParser.getRemainingArgs();
    if ((remainingArgs.length != 2) && (remainingArgs.length != 4)) {
	System.err.println("Usage: confidence <in> <out> [-skip skipPatternFile]");
	System.exit(2);
    }
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
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
