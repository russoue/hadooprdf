package edu.utdallas.hadooprdf.accesscontrol;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

public class PolicyBuilderLoader {
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length < 1) {
			System.out.println("Please give input file.");
			System.exit(1);
		}
		
	    File file = new File(args[0]);
	    FileInputStream fis = null;
	    BufferedInputStream bis = null;
	    DataInputStream dis = null;

	    try {
	      fis = new FileInputStream(file);

	      // Here BufferedInputStream is added for fast reading.
	      bis = new BufferedInputStream(fis);
	      dis = new DataInputStream(bis);

	      // dis.available() returns 0 if the file does not have more lines.
	      while (dis.available() != 0) {

	      // this statement reads the line from the file and print it to
	        // the console.
	    	  String line =dis.readLine();
	    	  String lineSplit[] = line.split("	");//Tab Seperated
	    	  String fileName = lineSplit[0];
	    	  String groupName[] = lineSplit[1].split(" ");
	    	  //System.out.println(groupName.length);

	    	  PolicyBuilder policyBuilder = new PolicyBuilder();
	    	  policyBuilder.run(fileName, groupName);
	      }

	      // dispose all the resources after using them.
	      fis.close();
	      bis.close();
	      dis.close();

	    } catch (FileNotFoundException e) {
	      e.printStackTrace();
	    } catch (IOException e) {
	      e.printStackTrace();
	    }
	  }
}
