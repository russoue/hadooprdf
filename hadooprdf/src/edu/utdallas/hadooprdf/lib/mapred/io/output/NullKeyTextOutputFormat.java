package edu.utdallas.hadooprdf.lib.mapred.io.output;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * An OutputFormat class for outputting Text but ignoring the key
 * @author Mohammad Farhan Husain
 *
 */
public class NullKeyTextOutputFormat<K, V> extends TextOutputFormat<K, V> {
	protected static class NullKeyTextOutputFormatLineRecordWriter<K, V>
			extends	LineRecordWriter<K, V> {
		private static final String utf8 = "UTF-8";
	    private static final byte[] newline;
		static {
			try {
				newline = "\n".getBytes(utf8);
			} catch (UnsupportedEncodingException uee) {
				throw new IllegalArgumentException("can't find " + utf8
						+ " encoding");
			}
		}
		/**
		 * The class constructor
		 * @param out the output stream
		 */
		public NullKeyTextOutputFormatLineRecordWriter(DataOutputStream out) {
			super(out);
		}
	    /**
	     * Write the object to the byte stream, handling Text as a special
	     * case.
	     * @param o the object to print
	     * @throws IOException if the write throws, we pass it on
	     */
		private void writeObject(Object o) throws IOException {
			if (o instanceof Text) {
				Text to = (Text) o;
				out.write(to.getBytes(), 0, to.getLength());
			} else {
				out.write(o.toString().getBytes(utf8));
			}
		}
		/**
		 * Writes the value ignoring the key
		 */
		@Override
		public synchronized void write(K key, V value) throws IOException {
			if (value != null && !(value instanceof NullWritable))
				writeObject(value);
			out.write(newline);
		}
	}

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		Configuration conf = job.getConfiguration();
		boolean isCompressed = getCompressOutput(job);
		CompressionCodec codec = null;
		String extension = "";
		if (isCompressed) {
			Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(
					job, GzipCodec.class);
			codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass,
					conf);
			extension = codec.getDefaultExtension();
		}
		Path file = getDefaultWorkFile(job, extension);
		FileSystem fs = file.getFileSystem(conf);
		if (!isCompressed) {
			FSDataOutputStream fileOut = fs.create(file, false);
			return new NullKeyTextOutputFormatLineRecordWriter<K, V>(fileOut);
		} else {
			FSDataOutputStream fileOut = fs.create(file, false);
			return new NullKeyTextOutputFormatLineRecordWriter<K, V>(new DataOutputStream(codec
					.createOutputStream(fileOut)));
		}
	}
}
