package edu.utdallas.hadooprdf.rdf.uri.prefix;

import java.util.HashMap;
import java.util.Map;

/**
 * This class is a prefix tree for URIs ({@link http://tools.ietf.org/html/rfc3305})
 * 
 * @author Mohammad Farhan Husain
 * @version 1.0
 * @since 1.0
 */
public class URIPrefixTree {
	/**
	 * The prefix of prefixes
	 */
	private String m_sPrefixPrefix;
	/**
	 * The root of the forest
	 */
	private Map<Character, PrefixTreeNode> m_TreeRoots;
	/**
	 * The prefix map
	 */
	private Map<String, String> m_LongestCommonPrefixes;
	/**
	 * A boolean to keep track whether new URI is inserted after prefixes are generated
	 */
	private boolean m_bPrefixesValid;
	
	/**
	 * The default class constructor
	 */
	public URIPrefixTree(String sPrefixPrefix) {
		m_sPrefixPrefix = sPrefixPrefix;
		m_TreeRoots = new HashMap<Character, PrefixTreeNode> ();
		m_LongestCommonPrefixes = new HashMap<String, String> ();
		m_bPrefixesValid = false;
	}
	
	/**
	 * Get longest common prefixes in the forest and their replacement prefixes
	 * @return a Map where the replacement prefix is the key and the prefix to be replaced is the value
	 */
	public Map<String, String> getLongestCommonPrefixes() {
		if (!m_bPrefixesValid)
			generateLongestCommonPrefixes();
		return m_LongestCommonPrefixes;
	}
	
	/**
	 * Generates the replacement prefixes for longest common prefixes
	 */
	private void generateLongestCommonPrefixes() {
		m_LongestCommonPrefixes.clear();
		int iPrefixCount = 0;
		for (PrefixTreeNode node : m_TreeRoots.values()) {
			String sReplacementPrefix = m_sPrefixPrefix + iPrefixCount;
			StringBuffer sbPrefix = new StringBuffer();
			while (null != node) {
				sbPrefix.append(node.getCharacter());
				node = (node.isEndOfWord()) ? null : node.getSingleChild();
			}
			if (sReplacementPrefix.length() < sbPrefix.length()) {
				m_LongestCommonPrefixes.put(sReplacementPrefix, sbPrefix.toString());
				iPrefixCount++;
			}
		}
		m_bPrefixesValid = true;
	}

	/**
	 * This static method checks whether a string is a URI of non-zero length {@link http://tools.ietf.org/html/rfc3305}
	 * 
	 * @param s the string to be checked.
	 * @return	true if s is a URI, otherwise returns false.
	 */
	public static boolean isURI(String s) {
		if (s.length() == 0) return false;
		return true;
	}
	
	/**
	 * Adds an URI to the prefix tree
	 * 
	 * @param s
	 * @throws InvalidURIException
	 */
	public void addURI(String s) throws InvalidURIException {
		if (!isURI(s)) throw new InvalidURIException(s);
		PrefixTreeNode node = m_TreeRoots.get(s.charAt(0));
		if (null == node) {
			node = new PrefixTreeNode(s.charAt(0));
			m_TreeRoots.put(s.charAt(0), node);
		}
		node.addChild(s, 1);
		m_bPrefixesValid = false;
	}
	
	/**
	 * Removes an URI from the prefix tree
	 * 
	 * @param s
	 * @throws InvalidURIException
	 */
	public void removeURI(String s) throws InvalidURIException {
		if (!isURI(s)) throw new InvalidURIException(s);
	}
	
	/**
	 * Prints the entire tree
	 */
	public void printTree(char chWhiteSpace) {
		for (PrefixTreeNode root : m_TreeRoots.values())
			root.printTree("", chWhiteSpace);
	}
}
