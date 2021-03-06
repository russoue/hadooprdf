package edu.utdallas.hadooprdf.query.parser;

import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mindswap.pellet.owlapi.Reasoner;
import org.semanticweb.owl.apibinding.OWLManager;
import org.semanticweb.owl.model.OWLClass;
import org.semanticweb.owl.model.OWLDescription;
import org.semanticweb.owl.model.OWLObjectProperty;
import org.semanticweb.owl.model.OWLObjectPropertyExpression;
import org.semanticweb.owl.model.OWLOntologyCreationException;
import org.semanticweb.owl.model.OWLOntologyManager;

import edu.utdallas.hadooprdf.data.commons.Constants;
import edu.utdallas.hadooprdf.data.metadata.DataSet;
import edu.utdallas.hadooprdf.lib.util.PathFilterOnFilenameExtension;

class FileListGenerator 
{
	private OWLOntologyManager mManager = OWLManager.createOWLOntologyManager();
	private Reasoner mReasoner = new Reasoner(mManager);
	private DataSet dataset = null;
	public static boolean isInverse = false;

	public FileListGenerator (edu.utdallas.hadooprdf.query.parser.Query query, DataSet dataset ) throws OWLOntologyCreationException 
	{
		this.dataset = dataset;
		Set <String> keys = query.getNsPrefixKeySet();
		Iterator <String> keySet = keys.iterator();
		while (keySet.hasNext()) 
		{
			String key = keySet.next();
			String ontologyURI = query.getNsPrefix(key);
			try { mReasoner.loadOntology(mManager.loadOntology(URI.create(ontologyURI))); } 
			catch (OWLOntologyCreationException e) { throw e; }			
		}	
		mReasoner.getKB().realize();
	}

	public boolean isPredicateTransitive( String predicate )
	{
		Iterator<OWLObjectProperty> iterAllProp = mReasoner.getObjectProperties().iterator();
		while( iterAllProp.hasNext() )
		{
			OWLObjectProperty sProp = iterAllProp.next();
			if( sProp.toString().contains( predicate ) && sProp.isTransitive( mManager.getOntologies() ) )
				return true;
		}	
		return false;
	}

	public List<String> getFilesAssociatedWithTriple (String uri, String classAssociated, String prefix)
	{
		ArrayList<String> files = new ArrayList<String> ();

		edu.utdallas.hadooprdf.conf.Configuration config = null;
		org.apache.hadoop.conf.Configuration hadoopConfiguration = null;
		FileSystem fs = null;
		try
		{
			config = edu.utdallas.hadooprdf.conf.Configuration.getInstance();
			hadoopConfiguration = new org.apache.hadoop.conf.Configuration( config.getHadoopConfiguration() );
			fs = FileSystem.get( hadoopConfiguration ); 			

			if ( uri.contains(".owl") ) 
			{
				FileStatus [] fstatus = fs.listStatus( dataset.getPathToPOSData(), new PathFilterOnFilenameExtension(Constants.POS_EXTENSION) );
				for (int i = 0; i < fstatus.length; i++) 
				{
					if (!fstatus[i].isDir()) 
					{
						if( fstatus[i].getPath().getName().toString().equalsIgnoreCase( prefix + ".pos" ) )
							files.add( fstatus[i].getPath().getName().toString() );
					}
				}
			}
			else
			{
				FileStatus [] fstatus = fs.listStatus( dataset.getPathToPOSData(), new PathFilterOnFilenameExtension(Constants.POS_EXTENSION) );
				for (int i = 0; i < fstatus.length; i++) 
				{
					if (!fstatus[i].isDir()) 
					{
						if( fstatus[i].getPath().getName().toString().contains( classAssociated ) )
							files.add( fstatus[i].getPath().getName().toString() );
					}
				}			
			}

			OWLDescription mpredClass = mManager.getOWLDataFactory().getOWLClass( URI.create( uri ) );		
			
			//Checking for inverse sub-properties
			Iterator<OWLObjectProperty> iterAllProp = mReasoner.getObjectProperties().iterator();
			while( iterAllProp.hasNext() )
			{
				OWLObjectProperty sProp = iterAllProp.next();
				if( sProp.toString().contains( classAssociated.substring( 1 ) ) )
				{
					Iterator<OWLObjectPropertyExpression> iterInvProp = sProp.getInverses( mManager.getOntologies() ).iterator();
					while( iterInvProp.hasNext() )
					{
						OWLObjectPropertyExpression invProp = iterInvProp.next();
						Iterator<OWLObjectPropertyExpression> iterSubProp = invProp.getSubProperties( mManager.getOntologies() ).iterator();  
						while( iterSubProp.hasNext() )
						{
							OWLObjectPropertyExpression prop = iterSubProp.next();
							FileStatus [] fstatus = fs.listStatus( dataset.getPathToPOSData(), new PathFilterOnFilenameExtension(Constants.POS_EXTENSION) );
							for (int i = 0; i < fstatus.length; i++) 
							{
								if (!fstatus[i].isDir()) 
								{
									if( fstatus[i].getPath().getName().toString().contains( prop.toString() ) )
										files.add( fstatus[i].getPath().getName().toString() );
									FileListGenerator.isInverse = true;
								}
							}							
						}
					}
				}
			}

			//Checking for sub properties of a given property
			iterAllProp = mReasoner.getObjectProperties().iterator();
			while( iterAllProp.hasNext() )
			{
				OWLObjectProperty sProp = iterAllProp.next();
				if( sProp.toString().contains( classAssociated.substring( 1 ) ) )
				{
					Iterator<OWLObjectPropertyExpression> iterSubProp = sProp.getSubProperties( mManager.getOntologies() ).iterator();
					while( iterSubProp.hasNext() )
					{
						OWLObjectPropertyExpression subProp = iterSubProp.next();
						FileStatus [] fstatus = fs.listStatus( dataset.getPathToPOSData(), new PathFilterOnFilenameExtension(Constants.POS_EXTENSION) );
						for (int i = 0; i < fstatus.length; i++) 
						{
							if (!fstatus[i].isDir()) 
							{
								if( fstatus[i].getPath().getName().toString().contains( subProp.toString() ) )
									files.add( fstatus[i].getPath().getName().toString() );
							}
						}													
					}
				}
			}

			//Sub classes
			Iterator<OWLClass> iterAllClasses = mReasoner.getClasses().iterator();
			while( iterAllClasses.hasNext() )
			{
				OWLClass sClass = iterAllClasses.next();
				if( sClass.toString().equalsIgnoreCase( mpredClass.toString() ) || !mReasoner.isSubClassOf( sClass, mpredClass ) ) continue;
				String fileName = prefix.substring(0, prefix.lastIndexOf("#")) + "#" + sClass + ".pos";	

				if( fs.exists( new Path( dataset.getPathToPOSData(), fileName ) ) )
					files.add( fileName );
			}

			//Super classes
			if( files.size() == 0 )
			{
				Iterator<OWLDescription> iterSuperClasses = mpredClass.asOWLClass().getSuperClasses( mManager.getOntologies() ).iterator();
				while( iterSuperClasses.hasNext() )
				{
					Iterator<OWLDescription> iterSubClasses = iterSuperClasses.next().asOWLClass().getSubClasses( mManager.getOntologies() ).iterator();
					while( iterSubClasses.hasNext() )
					{
						OWLClass sClass = iterSubClasses.next().asOWLClass();
						if( sClass.toString().equalsIgnoreCase( mpredClass.toString() ) ) continue;
						String fileName = prefix.substring(0, prefix.lastIndexOf("#")) + "#" + sClass + ".pos";	

						if( fs.exists( new Path( dataset.getPathToPOSData(), fileName ) ) )
							files.add( fileName );					
					}
				}
			}
			if( files.size() == 0 ) files.add( prefix + ".pos" );				
		}
		catch( Exception e ) { e.printStackTrace(); }				
		return files;
	}
}