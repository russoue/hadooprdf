package edu.utdallas.hadooprdf.preprocessing.serialization.test;

import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import edu.utdallas.hadooprdf.commons.Constants.SerializationFormat;
import edu.utdallas.hadooprdf.conf.ConfigurationNotInitializedException;
import edu.utdallas.hadooprdf.metadata.DataFileExtensionNotSetException;
import edu.utdallas.hadooprdf.metadata.DataSet;
import edu.utdallas.hadooprdf.preprocessing.serialization.ConversionToNTriplesException;
import edu.utdallas.hadooprdf.preprocessing.serialization.ConvertToNTriples;

public class ConvertToNTriplesTest {

	@Test
	public void testDoConversion() {
		// Create cluster configuration
		org.apache.hadoop.conf.Configuration hadoopConfiguration = new Configuration();
		//String sConfDirectoryPath = "conf/SAIALLabCluster";
		String sConfDirectoryPath = "conf/SemanticWebLabCluster";
		hadoopConfiguration.addResource(new Path(sConfDirectoryPath + "/core-site.xml"));
		hadoopConfiguration.addResource(new Path(sConfDirectoryPath + "/hdfs-site.xml"));
		hadoopConfiguration.addResource(new Path(sConfDirectoryPath + "/mapred-site.xml"));
		// Create application configuration
		edu.utdallas.hadooprdf.conf.Configuration config =
			edu.utdallas.hadooprdf.conf.Configuration.createInstance(hadoopConfiguration);
		config.setNumberOfTaskTrackersInCluster(5); // 5 for semantic web lab, 10 for SAIAL lab
		try {
			DataSet ds = new DataSet("/user/farhan/hadooprdf/LUBM1");
			ds.setOriginalDataFilesExtension("owl");
			ConvertToNTriples ctn = new ConvertToNTriples(SerializationFormat.RDF_XML, ds);
			ctn.doConversion();
		} catch (ConversionToNTriplesException e) {
			System.err.println("ConversionToNTriplesException occurred while testing ConvertToNTriples.doConversion\n" + e.getMessage());
			e.printStackTrace();
			fail(e.getMessage());
		} catch (ConfigurationNotInitializedException e) {
			System.err.println("ConfigurationNotInitializedException occurred while testing ConvertToNTriples.doConversion\n" + e.getMessage());
			e.printStackTrace();
			fail(e.getMessage());
		} catch (IOException e) {
			System.err.println("IOException occurred while testing ConvertToNTriples.doConversion\n" + e.getMessage());
			e.printStackTrace();
			fail(e.getMessage());
		} catch (DataFileExtensionNotSetException e) {
			System.err.println("DataFileExtensionNotSetException occurred while testing ConvertToNTriples.doConversion\n" + e.getMessage());
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

}
