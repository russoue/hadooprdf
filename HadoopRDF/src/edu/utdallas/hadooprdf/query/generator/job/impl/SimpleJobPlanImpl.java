package edu.utdallas.hadooprdf.query.generator.job.impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapreduce.Job;

import edu.utdallas.hadooprdf.query.generator.job.JobPlan;
import edu.utdallas.hadooprdf.query.generator.triplepattern.TriplePattern;

/**
 * A simple implementation of the job plan interface
 * @author vaibhav
 *
 */
public class SimpleJobPlanImpl implements JobPlan, Serializable
{	
	/** The serial version UID **/
	private static final long serialVersionUID = -8660692800623708427L;

	/** A map of predicates and the associated triple pattern **/
	private Map<String,TriplePattern> predTrPatternMap = new HashMap<String,TriplePattern>();
	
	/** The total number of variables expected in the result of the query **/
	private int totalVars = 0;
	
	/** The Hadoop Job object used by the current plan **/
	private transient Job currJob = null;
	
	/** A boolean that denotes if there are more jobs to follow the current one **/
	private boolean hasMoreJobs = false;
	
	/** A map between variables and the associated count of triple patterns that contain that variables **/
	private Map<String,Integer> varTrPatternCount = new HashMap<String,Integer>();
	
	/** The list of joining variables **/
	private List<String> joiningVars = new ArrayList<String>();
	
	/** The job identifier **/
	private int jobId = 0;
	
	/** The list of variables in the SELECT clause **/
	private List<String> listVarsSelectClause = new ArrayList<String>();
	
	/** Constructor **/
	public SimpleJobPlanImpl() { }

	/**
	 * Overrides the .toString() method so that this Object can be Serialized
	 */
	public String toString()
	{
		return "#1" + predTrPatternMap.toString() + "#1" + totalVars + " " + "" + 
		hasMoreJobs + "#2" + varTrPatternCount + "#2" + "l1" + joiningVars.toString() + "l1" + 
		"l2" + listVarsSelectClause.toString() + "l2" ; 
	}
	
	/**
	 * {@link edu.utdallas.hadooprdf.query.generator.job.JobPlan#addPredicateBasedTriplePattern(String, TriplePattern)}
	 */	
	public void setPredicateBasedTriplePattern( String pred, TriplePattern tp ) { predTrPatternMap.put( pred, tp ); }
	
	/**
	 * {@link edu.utdallas.hadooprdf.query.generator.job.JobPlan#getPredicateBasedTriplePattern(String)}
	 */	
	public TriplePattern getPredicateBasedTriplePattern( String pred ) { return predTrPatternMap.get( pred ); }
	
	/**
	 * {@link edu.utdallas.hadooprdf.query.generator.job.JobPlan#setTotalVariables(int)}
	 */
	public void setTotalVariables( int totalVars ) { this.totalVars = totalVars; }
	
	/**
	 * {@link edu.utdallas.hadooprdf.query.generator.job.JobPlan#getTotalVariables()}
	 */
	public int getTotalVariables() { return totalVars; }

	/**
	 * {@link edu.utdallas.hadooprdf.query.generator.job.JobPlan#setHadoopJob(Job)}
	 */
	public void setHadoopJob( Job currJob ) { this.currJob = currJob; }
	
	/**
	 * {@link edu.utdallas.hadooprdf.query.generator.job.JobPlan#getHadoopJob()}
	 */
	public Job getHadoopJob() { return currJob; }

	/**
	 * {@link edu.utdallas.hadooprdf.query.generator.job.JobPlan#setHasMoreJobs(boolean)}
	 */
	public void setHasMoreJobs( boolean hasMoreJobs ) { this.hasMoreJobs = hasMoreJobs; }
	
	/**
	 * {@link edu.utdallas.hadooprdf.query.generator.job.JobPlan#getHasMoreJobs()}
	 */
	public boolean getHasMoreJobs() { return hasMoreJobs; }

	/**
	 * {@link edu.utdallas.hadooprdf.query.generator.job.JobPlan#setVarTrPatternCount(String, Integer)}
	 */
	public void setVarTrPatternCount( String var, Integer count ) { varTrPatternCount.put( var, count ); }
	
	/**
	 * {@link edu.utdallas.hadooprdf.query.generator.job.JobPlan#getVarTrPatternCount(String)}
	 */
	public Integer getVarTrPatternCount( String var ) { return varTrPatternCount.get( var ); }
	
	/**
	 * {@link edu.utdallas.hadooprdf.query.generator.job.JobPlan#addVarToJoiningVariables(String)}
	 */
	public void addVarToJoiningVariables( String var ) { joiningVars.add( var ); }
	
	/**
	 * {@link edu.utdallas.hadooprdf.query.generator.job.JobPlan#getJoiningVariablesList()}
	 */
	public List<String> getJoiningVariablesList() { return joiningVars; }
	
	/**
	 * {@link edu.utdallas.hadooprdf.query.generator.job.JobPlan#setJobId(int)}
	 */
	public void setJobId( int jobId ) { this.jobId = jobId; }
	
	/**
	 * {@link edu.utdallas.hadooprdf.query.generator.job.JobPlan#getJobId()}
	 */
	public int getJobId() { return jobId; }
	
	/**
	 * {@link edu.utdallas.hadooprdf.query.generator.job.JobPlan#setSelectClauseVarList(List)}
	 */
	public void setSelectClauseVarList( List<String> listVars ) { this.listVarsSelectClause = listVars; }
	
	/**
	 * {@link edu.utdallas.hadooprdf.query.generator.job.JobPlan#getSelectClauseVarList()}
	 */
	public List<String> getSelectClauseVarList() { return listVarsSelectClause; }
}