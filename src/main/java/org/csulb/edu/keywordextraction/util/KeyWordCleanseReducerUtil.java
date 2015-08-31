package org.csulb.edu.keywordextraction.util;

import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class KeyWordCleanseReducerUtil {

	//Add entries to Map
	public Map<String, Double> addEntry(Map<String, Double> tagMap, Writable writableKey, Writable writableValue) {
		double previousValue = 0;
		Text key = (Text) writableKey;
		IntWritable value = (IntWritable) writableValue;
		if (tagMap.containsKey(key.toString())) {
			previousValue = tagMap.get(key.toString());
		}
		tagMap.put(key.toString(), previousValue + logFrequencyWeighting(value.get()));
		return tagMap;
	}

	public double logFrequencyWeighting(int termFrequency) {
		// 1 + log(tf)
		return (KeyWordExtractionConstants.ONE + Math.log(termFrequency));
	}
	
	public double invertedDocumentFrequency(long totalDocuments,long documentFrequency){
		// log(N/df)
		return Math.log(totalDocuments/documentFrequency);
	}
}

