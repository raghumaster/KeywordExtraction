package org.csulb.edu.keywordextraction.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.csulb.edu.keywordextraction.mapreduce.KeyWordExtractionDriver.KeyWordExtractionCounters;
import org.csulb.edu.keywordextraction.util.KeyWordCleanseReducerUtil;
import org.csulb.edu.keywordextraction.util.KeyWordExtractionConstants;

public class KeyWordExtractionCleanseStemReducer extends Reducer<Text, MapWritable, Text, MapWritable> {

	private long documentCount = 0;
	private Map<String, Double> tagMap;
	private KeyWordCleanseReducerUtil reduceUtil;
	private long totalDocumentCount = 0;
	private double invertedDocumentFrequency = 0;
	private MapWritable tagMapWritable;

	@Override
	protected void cleanup(Reducer<Text, MapWritable, Text, MapWritable>.Context context) throws IOException,
			InterruptedException {

	}

	@Override
	protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException,
			InterruptedException {
		documentCount = 0;
		tagMap = new HashMap<>();
		tagMapWritable = new MapWritable();
		Iterator<MapWritable> itr = values.iterator();
		Iterator<Entry<Writable, Writable>> mapIterator;
		// Input to reduce function will be Token, List<Map<Tag,Frequency>>
		while (itr.hasNext()) {
			documentCount++;
			mapIterator = itr.next().entrySet().iterator();
			while (mapIterator.hasNext()) {
				Entry<Writable, Writable> entry = mapIterator.next();
				tagMap = reduceUtil.addEntry(tagMap, entry.getKey(), entry.getValue());
			}
		}

		invertedDocumentFrequency = reduceUtil.invertedDocumentFrequency(totalDocumentCount, documentCount);

		for (String tag : tagMap.keySet()) {
			// Compute the tf-idf weighting
			tagMapWritable.put(new Text(tag), new DoubleWritable((tagMap.get(tag) * invertedDocumentFrequency)));
		}
		tagMapWritable.put(new Text(KeyWordExtractionConstants.DOCUMENTCOUNT), new LongWritable(documentCount));
		context.write(key, tagMapWritable);
	}

	@Override
	protected void setup(Reducer<Text, MapWritable, Text, MapWritable>.Context context) throws IOException,
			InterruptedException {
		// Initialize the ReduceUtil
		reduceUtil = new KeyWordCleanseReducerUtil();
		// Get the total document count from the global counters
		totalDocumentCount = context.getCounter(KeyWordExtractionCounters.TOTALDOCUMENTS).getValue();
	}

}
