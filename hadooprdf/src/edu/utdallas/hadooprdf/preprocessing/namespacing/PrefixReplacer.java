package edu.utdallas.hadooprdf.preprocessing.namespacing;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.utdallas.hadooprdf.commons.Tags;
import edu.utdallas.hadooprdf.conf.ConfigurationNotInitializedException;
import edu.utdallas.hadooprdf.lib.mapred.io.output.FilenameByKeyMultipleTextOutputFormat;
import edu.utdallas.hadooprdf.lib.util.PathFilterOnFilenameExtension;
import edu.utdallas.hadooprdf.metadata.DataFileExtensionNotSetException;
import edu.utdallas.hadooprdf.metadata.DataSet;
import edu.utdallas.hadooprdf.preprocessing.lib.PreprocessorJobRunner;

/**
 * A class to replace prefixes by namespaces found by PrefixFinder
 * @author Mohammad Farhan Husain
 */
public class PrefixReplacer extends PreprocessorJobRunner {
	/**
	 * The class constructor
	 * @param dataSet the data set to find the prefix for
	 * @throws DataFileExtensionNotSetException 
	 */
	public PrefixReplacer(DataSet dataSet) throws DataFileExtensionNotSetException {
		super(dataSet);
		m_InputDirectoryPath = dataSet.getPathToNTriplesData();
		m_OutputDirectoryPath = new Path(m_DataSet.getPathToTemp(), "replaced");
	}
	/**
	 * The method which runs the job for replacing prefixes with namespaces
	 * @throws ConfigurationNotInitializedException
	 * @throws PrefixReplacerException
	 */
	public boolean replacePrefixes() throws ConfigurationNotInitializedException, PrefixReplacerException {
		try {
			edu.utdallas.hadooprdf.conf.Configuration config =
				edu.utdallas.hadooprdf.conf.Configuration.getInstance();
			org.apache.hadoop.conf.Configuration hadoopConfiguration =
				new org.apache.hadoop.conf.Configuration(config.getHadoopConfiguration()); // Should create a clone so
			// that the original one does not get cluttered with job specific key-value pairs
			FileSystem fs;
			fs = FileSystem.get(hadoopConfiguration);
			// Delete output directory
			fs.delete(m_OutputDirectoryPath, true);
			// Must set all the job parameters before creating the job
			String sPathToPrefixFile = m_DataSet.getPathToPrefixFile().getParent().toString() + '/' + m_DataSet.getPathToPrefixFile().getName();
			hadoopConfiguration.set(Tags.PATH_TO_PREFIX_FILE, sPathToPrefixFile);
			// Create the job
			String sJobName = "Prefix Replacer for " + m_InputDirectoryPath.getParent() + '/' + m_InputDirectoryPath.getName();
			Job job = new Job(hadoopConfiguration, sJobName);
			// Specify input parameters
			job.setInputFormatClass(TextInputFormat.class);
			boolean bInputPathEmpty = true;
			// Get input file names
			FileStatus [] fstatus = fs.listStatus(m_InputDirectoryPath, new PathFilterOnFilenameExtension(m_sInputFilesExtension));
			for (int i = 0; i < fstatus.length; i++) {
				if (!fstatus[i].isDir()) {
					FileInputFormat.addInputPath(job, fstatus[i].getPath());
					bInputPathEmpty = false;
				}
			}
			if (bInputPathEmpty)
				throw new PrefixReplacerException("No file to replace prefix for!");
			// Specify output parameters
			job.setOutputFormatClass(FilenameByKeyMultipleTextOutputFormat.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			FileOutputFormat.setOutputPath(job, m_OutputDirectoryPath);
			// Set the mapper and reducer classes
			job.setMapperClass(edu.utdallas.hadooprdf.preprocessing.namespacing.mapred.PrefixReplacerMapper.class);
			job.setReducerClass(edu.utdallas.hadooprdf.preprocessing.namespacing.mapred.PrefixReplacerReducer.class);
			// Set the number of reducers
			if (-1 != getNumberOfReducers())	// Use the number set by the client, if any
				job.setNumReduceTasks(getNumberOfReducers());
			else if (-1 != config.getNumberOfTaskTrackersInCluster())	// Use one reducer per TastTracker, if the number of TaskTrackers is available
				job.setNumReduceTasks(config.getNumberOfTaskTrackersInCluster());
			// Set the jar file
			job.setJarByClass(this.getClass());
			// Run the job
			if (job.waitForCompletion(true)) {
				fs.delete(m_InputDirectoryPath, true);
				fs.mkdirs(m_InputDirectoryPath);
				fstatus = fs.listStatus(m_InputDirectoryPath, new PathFilterOnFilenameExtension(m_sInputFilesExtension));
				for (int i = 0; i < fstatus.length; i++)
					fs.rename(fstatus[i].getPath(), new Path(m_InputDirectoryPath, fstatus[i].getPath().getName()));
				fs.delete(m_OutputDirectoryPath, true);
				return true;
			}
			return false;
		} catch (IOException e) {
			throw new PrefixReplacerException("IOException occurred:\n" + e.getMessage());
		} catch (InterruptedException e) {
			throw new PrefixReplacerException("InterruptedException occurred:\n" + e.getMessage());
		} catch (ClassNotFoundException e) {
			throw new PrefixReplacerException("ClassNotFoundException occurred:\n" + e.getMessage());
		}
	}
}
