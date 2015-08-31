package org.csulb.edu.keywordextraction.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.csulb.edu.keywordextraction.util.KeyWordCleanseMapperUtil;
import org.csulb.edu.keywordextraction.util.KeyWordExtractionConstants;
import org.csulb.edu.keywordextraction.util.Posting;

/**
 * @author Raghu Nandan & Goutam Tadi This mapper class reads test data and
 *         emits token as key and question id as value.
 */
public class KeyWordExtractionTestingTokenTFIDFMapper extends Mapper<LongWritable, Text, Text, MapWritable> {

	Posting posting;
	Set<String> stopWordsSet;
	KeyWordCleanseMapperUtil mapperUtil;
	Iterator<Map.Entry<String, Integer>> entries;
	String questionID;
	MapWritable mapWritable;

	@Override
	protected void cleanup(Mapper<LongWritable, Text, Text, MapWritable>.Context context) throws IOException,
			InterruptedException {

	}

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, MapWritable>.Context context)
			throws IOException, InterruptedException {
		// QuestionID, Question Title, Question Body
		posting = new Posting(value.toString(), stopWordsSet, KeyWordExtractionConstants.FALSE);
		entries = posting.getTokens().entrySet().iterator();
		questionID = Long.toString(posting.getId());

		while (entries.hasNext()) {
			Map.Entry<String, Integer> entry = entries.next();
			// Write to the context only if the token is not null or ""
			if (!entry.getKey().equals(KeyWordExtractionConstants.EMPTY)) {
				mapWritable = new MapWritable();
				mapWritable.put(new Text(questionID), new IntWritable(entry.getValue()));
				context.write(new Text(entry.getKey().trim()), mapWritable);
			}
		}

	}

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, MapWritable>.Context context) throws IOException,
			InterruptedException {
		mapperUtil = new KeyWordCleanseMapperUtil();
		// Add all the stop words to the stop word set
		stopWordsSet = mapperUtil.generateStopWords(mapperUtil.getCacheFiles(context));
	}

}
