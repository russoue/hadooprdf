package edu.utdallas.hadooprdf.query.generator.triplepattern;

import com.hp.hpl.jena.graph.Node;

/**
 * An interface for different triple patterns that are used in a job plan
 * @author sharath, vaibhav
 *
 */
public interface TriplePattern 
{
	/**
	 * A method to set the identifier for the current triple pattern
	 * @param id - the identifier of this triple pattern
	 */
	public void setTriplePatternId( int id ) ;
	
	/**
	 * A method that returns the current triple pattern's identifier
	 * @return the current triple pattern's identifier
	 */
	public int getTriplePatternId() ;
	
	/**
	 * A method that sets the position of the joining variable in the current triple pattern
	 * @param var - the position of the joining variable, "s" or "o"
	 */
	public void setJoiningVariable( String var ) ;
	
	/**
	 * A method that returns the position of the joining variable in the current triple pattern
	 * @return the position of the joining variable in the current triple pattern 
	 */
	public String getJoiningVariable() ;
	
	/**
	 * A method that sets the number of variables in the current triple pattern
	 * @param numOfVar - the number of variables in this triple pattern
	 */
	public void setNumOfVariables( int numOfVar ) ;
	
	/**
	 * A method that returns the number of variables in the current triple pattern
	 * @return the number of variables in the current triple pattern
	 */
	public int getNumOfVariables() ;
	
	/**
	 * A method that sets the mapping between a filename and its associated prefix
	 * @param filename - the filename, i.e. the predicate in every triple pattern
	 * @param prefix - the prefix associated with that filename
	 */
	public void setFilenameBasedPrefix( String filename, String prefix ) ;
	
	/**
	 * A method that returns the prefix associated with the input filename
	 * @param filename - the filename whose prefix is required
	 * @return the associated prefix
	 */
	public String getFilenameBasedPrefix( String filename ) ;
	
	/**
	 * A method that checks if a prefix exists
	 * @param prefix - the prefix to be checked
	 * @return true iff the prefix exists, false otherwise
	 */
	public boolean checkIfPrefixExists( String prefix ) ;
	
	/**
	 * A method that sets for every triple pattern, if it has one a literal value
	 * @param literal - the literal value if present in a triple pattern
	 */
	public void setLiteralValue( String literal ) ;
	
	/**
	 * A method that returns the literal value associated with the current triple pattern
	 * @return - the literal value associated with this triple pattern
	 */
	public String getLiteralValue() ;

	/**
	 * A method that sets the value of the subject for this triple pattern
	 * @param pred - the subject for this triple pattern
	 */
	public void setSubjectValue( Node sub ) ;
	
	/**
	 * A method that returns the subject in the current triple pattern
	 * @return the subject for the current triple pattern
	 */
	public Node getSubjectValue() ;
	
	
	/**
	 * A method that sets the value of the predicate for this triple pattern
	 * @param pred - the predicate for this triple pattern
	 */
	public void setPredicateValue( Node pred ) ;
	
	/**
	 * A method that returns the predicate in the current triple pattern
	 * @return the predicate for the current triple pattern
	 */
	public Node getPredicateValue() ;
	
	/**
	 * A method that sets the value of the object for this triple pattern
	 * @param pred - the object for this triple pattern
	 */
	public void setObjectValue( Node obj ) ;
	
	/**
	 * A method that returns the object in the current triple pattern
	 * @return the object for the current triple pattern
	 */
	public Node getObjectValue() ;
	
	/**
	 * A method that sets the value of the joining variable in the current triple pattern
	 * @param val - the value of the joining variable in this triple pattern
	 */
	public void setJoiningVariableValue( String val ) ;
	
	/**
	 * A method that returns the value of the joining variable in the current triple pattern
	 * @return the value of the joining variable in this triple pattern
	 */
	public String getJoiningVariableValue() ;
}