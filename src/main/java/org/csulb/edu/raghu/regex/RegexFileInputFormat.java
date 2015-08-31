package org.csulb.edu.raghu.regex;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class RegexFileInputFormat extends FileInputFormat<LongWritable, Text> {

	public static String REGEX_RECORD_SEPARATOR = "regex.record.separator";
	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException, InterruptedException {
		
		String regexseparator = context.getConfiguration().get(REGEX_RECORD_SEPARATOR);
		return new CustomRegexRecordReader(regexseparator);
		
		
	}

}
