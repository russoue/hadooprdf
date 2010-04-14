package edu.utdallas.hadooprdf.rdf.uri.prefix.test;

import junit.framework.TestCase;
import edu.utdallas.hadooprdf.rdf.uri.prefix.InvalidURIException;
import edu.utdallas.hadooprdf.rdf.uri.prefix.URIPrefixTree;

public class URIPrefixTreeTest extends TestCase {

	protected void setUp() throws Exception {
		super.setUp();
	}
	
	public void testPrefixTree() {
		URIPrefixTree tree = new URIPrefixTree("p");
		try {
			tree.addURI("abcd");
			tree.addURI("abce");
			tree.addURI("bbce");
			tree.addURI("bdce");
			tree.addURI("e");
			tree.addURI("bdcf");
			tree.addURI("mnop");
			tree.addURI("mnopq");
			tree.printTree('-');
			System.out.println(tree.getLongestCommonPrefixes());
		} catch (InvalidURIException e) {
			System.err.println(e.getMessage());
		}
	}

	protected void tearDown() throws Exception {
		super.tearDown();
	}

}
