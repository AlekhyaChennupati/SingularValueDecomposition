package org.myorg;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.myorg.NaiveMain.COUNTERS_LIST;

public class Reduce extends Reducer<Text,Text,Text,Text> {
	public void reduce(Text label,Iterable<Text> documents, Context context)
			throws IOException, InterruptedException{
		Text docs = null;
		
		
		//while(((Object) documents).hasNext())
		for(Text docu : documents)
		{
			System.out.println("TEXT DOCs="+docu);
			docs=docu;
			if(label.toString().equals("CCAT"))
			{
				System.out.println("Inside label="+label);
				context.getCounter(COUNTERS_LIST.No_Of_CCAT_Docs).increment(1);
			}
			if(label.toString().equals("ECAT"))
			{
				System.out.println("Inside label="+label);
				context.getCounter(COUNTERS_LIST.No_Of_ECAT_Docs).increment(1);
			}
			if(label.toString().equals("GCAT"))
			{
				System.out.println("Inside label="+label);
				context.getCounter(COUNTERS_LIST.No_Of_GCAT_Docs).increment(1);
			}
			if(label.toString().equals("MCAT"))
			{
				System.out.println("Inside label="+label);
				context.getCounter(COUNTERS_LIST.No_Of_MCAT_Docs).increment(1);
			}
		}
		
		context.getCounter(COUNTERS_LIST.No_Of_Labels).increment(1);
		context.write(label, docs);
		
	}
	
}
