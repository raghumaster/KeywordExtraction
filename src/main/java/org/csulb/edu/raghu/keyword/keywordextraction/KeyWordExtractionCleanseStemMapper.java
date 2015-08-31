package org.csulb.edu.raghu.keyword.keywordextraction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.csulb.edu.raghu.keyword.keywordextraction.KeyWordExtractionDriver.KeyWordExtractionCounters;
import org.csulb.edu.raghu.keyword.util.KeyWordCleanseMapperUtil;
import org.csulb.edu.raghu.keyword.util.KeyWordExtractionConstants;
import org.csulb.edu.raghu.keyword.util.Posting;

/*
 * @author Raghu Nandan & Goutam Tadi
 * Mapper class for 
 * 		1. reading the data from training data set 
 * 		2. Creates a new Posting
 * 		3. Writes the token & inverted tag freq map to the context
 */
public class KeyWordExtractionCleanseStemMapper extends Mapper<LongWritable, Text, Text, MapWritable> {

	Posting posting;
	Iterator<String> listIterator;
	MapWritable mapWritable;
	Set<String> stopWordsSet;
	KeyWordCleanseMapperUtil mapperUtil;

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {

	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// Global counter for keeping track of total number of documents
		context.getCounter(KeyWordExtractionCounters.TOTALDOCUMENTS).increment(KeyWordExtractionConstants.ONE);
		// Create a new posting using the training data
		// true - parameter indicates the input data is training data
		posting = new Posting(value.toString(), stopWordsSet, KeyWordExtractionConstants.TRUE);
		Iterator<Map.Entry<String, Integer>> entries = posting.getTokens().entrySet().iterator();
		/*
		 * Posting1 Sample Input [token1:4, token2:3, token3:1 ... ]
		 * [tag1,tag2,tag3]
		 */
		while (entries.hasNext()) {
			Map.Entry<String, Integer> entry = entries.next();
			mapWritable = new MapWritable();
			listIterator = posting.getTags().iterator();
			/*
			 * Invert the token tag freq (mapWritable object) token1 : [tag1:4,
			 * tag2:4, tag3:4] token2 : [tag1:3, tag2:3, tag3:3] token3 :
			 * [tag1:1, tag2:1, tag3:1]
			 */
			while (listIterator.hasNext()) {
				mapWritable.put(new Text(listIterator.next()), new IntWritable(entry.getValue()));

			}
			// Write to the context only if the token is not null or ""
			if (!entry.getKey().equals(KeyWordExtractionConstants.EMPTY)) {
				context.write(new Text(entry.getKey().trim()), mapWritable);
			}
		}
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		mapperUtil = new KeyWordCleanseMapperUtil();
		// Add all the stop words to the stop word set
		stopWordsSet = mapperUtil.generateStopWords(mapperUtil.getCacheFiles(context));
	}

}
