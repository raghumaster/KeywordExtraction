package org.csulb.edu.raghu.regex;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class CustomRegexRecordReader extends RecordReader<LongWritable, Text> {

	LineRecordReader recReader = new LineRecordReader();
	private LongWritable key;
	Pattern pattern ;
	Matcher match;
	StringBuilder sbr = null;
	boolean loopFlag = true;
	float progress = 0.0F;

	private Text value;
	String line = null;

	public CustomRegexRecordReader(String regexseparator) {
		
		pattern = Pattern.compile(regexseparator);
	}

	@Override
	public void close() throws IOException {
		recReader.close();

	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return recReader.getProgress();
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {

		recReader.initialize(split, context);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {

		while (loopFlag) {
			if (line != null) {

				if (lineMatches(line)) {
					if (sbr != null) {
						key = new LongWritable();
						value = new Text(sbr.toString());
						sbr = null;

						return true;
					}
					sbr = new StringBuilder();
					sbr.append(line.trim());
				} else {
					sbr.append(line.trim());
				}

				if (recReader.getProgress() >= 1.0F) {
					loopFlag = false;
					key = new LongWritable();
					value = new Text(sbr.toString());
					return true;
				}
				line = nextValue();
			} else {
				line = nextValue();
			}

		}
		return loopFlag;

	}

	public boolean lineMatches(String string) {
		if (string != null) {
			match = pattern.matcher(string);
			return match.matches();
		}
		return false;

	}

	public String nextValue() throws IOException {
		recReader.nextKeyValue();
		return recReader.getCurrentValue().toString();
	}
}
