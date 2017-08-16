package org.myorg;

import java.io.IOException;
import java.io.File;
import org.apache.commons.io.FileUtils;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
//import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.JobInProgress.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.net.*;
import org.apache.hadoop.filecache.*;




public class NaiveMain extends Configured implements Tool{
	
	public static enum COUNTERS_LIST {
		//  INCOMING_GRAPHS,
		  No_Of_Labels,
		  No_Of_Docs,
		  No_Of_CCAT_Docs,
		  No_Of_ECAT_Docs,
		  No_Of_GCAT_Docs,
		  No_Of_MCAT_Docs;
		};
	

	private static final Logger Log = Logger.getLogger(NaiveMain.class);
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new NaiveMain(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception {
		
		//String s3File = args[2];
		Job parseJob = Job.getInstance(getConf(),"Parsing");
		parseJob.setJarByClass(this.getClass());
		FileInputFormat.addInputPath(parseJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(parseJob, new Path(args[1]));
	//	DistributedCache.addCacheFile(new URI(s3File), getConf());
		parseJob.setMapperClass(ParseMapper.class);
		parseJob.setCombinerClass(Reduce.class);
		//parseJob.setReducer
		parseJob.setOutputKeyClass(Text.class);
		parseJob.setOutputValueClass(Text.class);
	//	parseJob.se
	//	parseJob.setInputFormatClass(TextInputFormat.class);
   //	parseJob.setOutputFormatClass(TextOutputFormat.class);
		
	//	parseJob.addinputchache(new URI(s3File));
	 //   DistributedCache.addCacheFile(new URI(s3File), getConf());
		int result = parseJob.waitForCompletion(true) ? 0 : 1;
	
		Counters counters = parseJob.getCounters();
		org.apache.hadoop.mapreduce.Counter c1 = counters.findCounter(COUNTERS_LIST.No_Of_Docs);
		System.out.println(c1.getDisplayName()+":"+c1.getValue());
		
		org.apache.hadoop.mapreduce.Counter c2 = counters.findCounter(COUNTERS_LIST.No_Of_Labels);
		System.out.println(c2.getDisplayName()+":"+c2.getValue());
		
		org.apache.hadoop.mapreduce.Counter c3 = counters.findCounter(COUNTERS_LIST.No_Of_CCAT_Docs);
		System.out.println(c3.getDisplayName()+":"+c3.getValue());
		
		org.apache.hadoop.mapreduce.Counter c4 = counters.findCounter(COUNTERS_LIST.No_Of_ECAT_Docs);
		System.out.println(c4.getDisplayName()+":"+c4.getValue());
	
		org.apache.hadoop.mapreduce.Counter c5 = counters.findCounter(COUNTERS_LIST.No_Of_GCAT_Docs);
		System.out.println(c5.getDisplayName()+":"+c5.getValue());
		
		org.apache.hadoop.mapreduce.Counter c6 = counters.findCounter(COUNTERS_LIST.No_Of_MCAT_Docs);
		System.out.println(c6.getDisplayName()+":"+c6.getValue());
	
		return result;
	}
}
