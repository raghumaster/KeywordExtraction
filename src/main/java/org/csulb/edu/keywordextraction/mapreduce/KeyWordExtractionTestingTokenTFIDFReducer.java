package org.csulb.edu.keywordextraction.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KeyWordExtractionTestingTokenTFIDFReducer extends Reducer<Text, MapWritable, Text, MapWritable> {

	@Override
	protected void cleanup(Reducer<Text, MapWritable, Text, MapWritable>.Context context) throws IOException,
			InterruptedException {

	}

	@Override
	protected void reduce(Text arg0, Iterable<MapWritable> arg1,
			Reducer<Text, MapWritable, Text, MapWritable>.Context arg2) throws IOException, InterruptedException {

	}

	@Override
	protected void setup(Reducer<Text, MapWritable, Text, MapWritable>.Context context) throws IOException,
			InterruptedException {

	}

}
