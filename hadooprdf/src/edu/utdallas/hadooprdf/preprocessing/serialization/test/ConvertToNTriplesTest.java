package edu.utdallas.hadooprdf.preprocessing.serialization.test;

import static org.junit.Assert.fail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import edu.utdallas.hadooprdf.commons.Constants.SerializationFormat;
import edu.utdallas.hadooprdf.conf.ConfigurationNotInitializedException;
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
		ConvertToNTriples ctn = new ConvertToNTriples(SerializationFormat.RDF_XML,
				new Path("/user/farhan/hadooprdf/LUBM1/Original"),
				new Path("/user/farhan/hadooprdf/LUBM1/NTriples"));
		try {
			ctn.doConversion();
		} catch (ConversionToNTriplesException e) {
			System.err.println("Exception occurred while testing ConvertToNTriples.doConversion\n" + e.getMessage());
			e.printStackTrace();
			fail(e.getMessage());
		} catch (ConfigurationNotInitializedException e) {
			System.err.println("Exception occurred while testing ConvertToNTriples.doConversion\n" + e.getMessage());
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

}
