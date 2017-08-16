package org.myorg;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;
 
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper; 
import org.myorg.NaiveMain.COUNTERS_LIST;

public class ParseMapper extends Mapper<LongWritable, Text, Text, Text> {
	public void setup(Context context) throws IOException, InterruptedException {
		System.out.println("I am here");
		//Configuration config = context.getConfiguration();
		
	/*	System.out.println("context.getcachefilelength="+context.getCacheFiles().length);
		if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
			URI mappingFileUri = context.getCacheFiles()[0];
			System.out.println("INSIDE PARSE MAPPER"+mappingFileUri.toString());
			if (mappingFileUri != null) {
			
				System.out.println("Mapping File: " + FileUtils.readFileToString(new File("./")));
			} else {
				System.out.println(">>>>>> NO MAPPING FILE");
			}
		} else {
			System.out.println(">>>>>> NO CACHE FILES AT ALL");
		}*/
	}
	public void map(LongWritable offset, Text value, Context context) throws IOException, InterruptedException
	{	
	String input = value.toString();
	String[] document=null;
	String[] Labels = null;
//	int noOfDocuments_Counter=0;
	Text current_Label= new Text();
	Text current_Document = new Text();
	//System.out.println("input is"+input);
		for( String line :input.split("\n"))
	{
	//System.out.println("Line is"+line);	
			document =line.split("\t");
			if(document.length>1)
			{
			//	System.out.println(document[0]);
				Labels = document[0].split(",");
				context.getCounter(COUNTERS_LIST.No_Of_Docs).increment(1);
			//	noOfDocuments_Counter++;
				for(int j=0;j<Labels.length;j++)
				{
				//	System.out.println("Key="+Labels[j]);
				//	System.out.println("Value="+document[1]);
					current_Label = new Text(Labels[j]);
					current_Document = new Text(document[1]);
					context.write(current_Label,current_Document);
				}
			}
		
	}
	//	System.out.println("No of docs="+noOfDocuments_Counter);
	
	}	
		
	}
 