package edu.utdallas.hadooprdf.rdf.uri.prefix;

import java.util.HashMap;
import java.util.Map;

/**
 * The class represents a node in the prefix tree
 * @author hadoop
 *
 */
public class PrefixTreeNode {
	/**
	 * The character of this node
	 */
	private char m_Character;
	/**
	 * The children nodes
	 */
	private Map<Character, PrefixTreeNode> m_Children;
	/**
	 * Count of the words which end at this node
	 */
	private int m_iWordEndsHere;
	
	/**
	 * The class constructor
	 * @param ch the character of the node
	 */
	public PrefixTreeNode(char ch) {
		m_Character = ch;
		m_Children = new HashMap<Character, PrefixTreeNode> ();
		m_iWordEndsHere = 0;
	}
	
	/**
	 * Adds a child node, if not present, and calls the same method of that node to add next child
	 * @param charArray
	 * @param index
	 */
	public void addChild(String s, int index) {
		if (index >= s.length()) {
			if (index == s.length())
				m_iWordEndsHere++;
			return;
		}
		char ch = s.charAt(index);
		PrefixTreeNode node = m_Children.get(ch);
		if (null == node) {
			node = new PrefixTreeNode(ch);
			m_Children.put(ch, node);
		}
		node.addChild(s, index + 1);
	}
	
	/**
	 * Get the character of the node
	 * @return the character of the node
	 */
	public char getCharacter() {
		return m_Character;
	}
	
	/**
	 * Is end of a word?
	 * @return true if end of a word
	 */
	public boolean isEndOfWord() {
		return 0 != m_iWordEndsHere;
	}
	
	/**
	 * Get the number of words which end at this node
	 * @return the number of words which end at this node
	 */
	public int getNumberOfWordsEndingHere() {
		return m_iWordEndsHere;
	}
	
	/**
	 * Get number of children
	 * @return number of children
	 */
	public int getNumberOfChildren() {
		return m_Children.size();
	}
	
	/**
	 * Gets the single child
	 * @return the single child, if the number of children is not 1, return null
	 */
	public PrefixTreeNode getSingleChild() {
		return (1 == m_Children.size()) ? m_Children.values().iterator().next() : null;
	}
	
	/**
	 * Prints the node and the subtree rooted at it
	 */
	public void printTree(String sPrecedingWhiteSpaces, char chWhiteSpace) {
		if (null == sPrecedingWhiteSpaces) sPrecedingWhiteSpaces = "";
		System.out.println(sPrecedingWhiteSpaces + m_Character);
		String sPrecedingWhiteSpacesForChildren = sPrecedingWhiteSpaces + chWhiteSpace;
		for (PrefixTreeNode child : m_Children.values()) {
			child.printTree(sPrecedingWhiteSpacesForChildren, chWhiteSpace);
		}
	}
}
