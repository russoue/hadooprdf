package edu.utdallas.hadooprdf.lib.mapred;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * An identify reducer for Text type
 * @author Mohammad Farhan Husain
 *
 */
public class IdentityReducer extends Reducer<Text, Text, Text, Text> {

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		Iterator<Text> iter = values.iterator();
		while (iter.hasNext())
			context.write(key, iter.next());
	}

}
