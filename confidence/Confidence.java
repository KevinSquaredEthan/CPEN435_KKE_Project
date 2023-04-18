import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Collections;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Confidence {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      ArrayList<String> A = new ArrayList<String>(); // String not Text, as sort works with strings
      StringTokenizer itr = new StringTokenizer(value.toString(),"\n"); // iterate through each line
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
    Job job = Job.getInstance(conf, "term pair count");
    job.setJarByClass(Confidence.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
