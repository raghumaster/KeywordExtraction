package org.csulb.edu.keywordextraction.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.csulb.edu.keywordextraction.regex.RegexFileInputFormat;
import org.csulb.edu.keywordextraction.util.KeyWordExtractionConstants;

public class KeyWordExtractionDriver extends Configured implements Tool {

	enum KeyWordExtractionCounters {
		TOTALDOCUMENTS
	}

	public int run(String[] args) throws Exception {

		Configuration configuration = this.getConf();

		// Deletes output path before the job starts.
		FileSystem fileSystem = FileSystem.get(configuration);
		fileSystem.delete(new Path(args[KeyWordExtractionConstants.ONE]), KeyWordExtractionConstants.TRUE);

		RemoteIterator<LocatedFileStatus> itr = fileSystem.listFiles(new Path(args[KeyWordExtractionConstants.TWO]),
				KeyWordExtractionConstants.TRUE);
		while (itr.hasNext()) {
			DistributedCache.addCacheFile(itr.next().getPath().toUri(), configuration);
		}

		Job job = new Job(configuration, KeyWordExtractionConstants.JOBNAME);
		job.setJarByClass(KeyWordExtractionDriver.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MapWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MapWritable.class);
		job.setMapperClass(KeyWordExtractionCleanseStemMapper.class);
		job.setReducerClass(KeyWordExtractionCleanseStemReducer.class);
		job.setNumReduceTasks(KeyWordExtractionConstants.ONE);
		job.setInputFormatClass(RegexFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[KeyWordExtractionConstants.ZERO]));
		FileOutputFormat.setOutputPath(job, new Path(args[KeyWordExtractionConstants.ONE]));

		return job.waitForCompletion(KeyWordExtractionConstants.TRUE) ? KeyWordExtractionConstants.ZERO
				: KeyWordExtractionConstants.ONE;

	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new KeyWordExtractionDriver(), args));
	}

}
