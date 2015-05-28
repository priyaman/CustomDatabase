package edu.buffalo.cse562.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;

import edu.buffalo.cse562.globals.GlobalConstants;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;

public class statsCollection {

	CreateTable stmt=null;
	Environment myDbEnvironment = null;
	Database myDatabase = null;
	DatabaseConfig dbConfig=null;
	EnvironmentConfig envConfig=null;
	public statsCollection()
	{
		envConfig = new EnvironmentConfig();
		envConfig.setAllowCreate(true);
		
	}
	public void parseCreateTable(CreateTable stmt)
	{
		this.stmt = stmt;
		ArrayList<Index> index = (ArrayList<Index>) stmt.getIndexes();
		for(int i=0;i<index.size();i++)
		{
			System.out.println(index.get(i).getName());
			System.out.println(index.get(i).getType());
			System.out.println(index.get(i).getColumnsNames());
		}
		String filename= stmt.getTable().getName();

		//TO DO :logic to identify which column in the index is primary and which is secondary


		/* for(int i=0;i<stmt.getColumnDefinitions().size();i++)
		 {
			 ColumnDefinition cd = (ColumnDefinition) stmt.getColumnDefinitions().get(i);
			 if(cd.getColumnName().equalsIgnoreCase(index.get(0).getColumnsNames().get(0).toString()));
			 colIndex=i;
		 }*/

		//Column for indexes
		//LINEITEM:  PRIMARY KEY (orderkey=1))  // shipdate=11, suppkey=3, discount=7, quantity=5, returnflag=9, shipmode=15, commitdate=12, receiptdate=13
		//ORDERS: PRIMARY KEY (orderkey=1)) //orderdate=5
		//PART:PRIMARY KEY (partkey=1))
		//CUSTOMER: PRIMARY KEY (custkey=1))  //mktsegment=7, nationkey=4
		//SUPPLIER: PRIMARY KEY (suppkey=1)) //nationkey=3
		//PARTSUPP: PRIMARY KEY (partkey=1, suppkey=1))
		//NATION: PRIMARY KEY (nationkey=1)) //regionkey=3
		//REGION: PRIMARY KEY (regionkey=1))  //name=2

		
		//Not doing part table
		ArrayList<String> colName = new ArrayList<String>();
		ArrayList<Integer> colIndex = new ArrayList<Integer>();

		if(filename.equalsIgnoreCase("CUSTOMER"))
		{
			colName.add("CUSTKEY");
			//colIndex.add(3);
			//colIndex.add(6);
		}
		else if(filename.equalsIgnoreCase("SUPPLIER"))
		{
			colName.add("SUPPKEY");			
			//colIndex.add(2);
		}
		/*else if(filename.equalsIgnoreCase("NATION"))
		{
			colIndex.add(0);			
			colIndex.add(2);
		}
		else if(filename.equalsIgnoreCase("REGION"))
		{
			colIndex.add(0);			
			colIndex.add(1);
		}*/
		else if(filename.equalsIgnoreCase("ORDERS"))
		{
			colName.add("ORDERKEY");			
			//colIndex.add(4);
		}
		else if(filename.equalsIgnoreCase("LINEITEM"))
		{
			//0,2,4,6,8,10,11,12
			colName.add("ORDERKEY");			
		//	colIndex.add(2);
			//colIndex.add(4);
			//colIndex.add(6);
			//colIndex.add(8);
			//colIndex.add(10);
			//colIndex.add(11);
			//colIndex.add(12);

		}
		colIndex.add(0);
		for(int i=0;i<colIndex.size();i++)		
		{
			//createDB(filename+"."+colName.get(i));
			createDB(filename);
			createIndex(filename, colIndex.get(i));
			
		}
		createSecDB("CUSTOMER.NATIONKEY");
		myDatabase.close();
	}
	public void createSecDB(String dbnameSec)
	{
		SecondaryConfig secConfig = new SecondaryConfig();
		TupleBinding myDataBinding = TupleBinding.getPrimitiveBinding(String.class);
		MyKeyCreator mykey = new MyKeyCreator(myDataBinding);
		secConfig.setKeyCreator(mykey);
		String secDBName = dbnameSec;
		SecondaryDatabase secondaryIndex = myDbEnvironment.openSecondaryDatabase(null, secDBName, myDatabase	, secConfig);
	}
	public Database createDB(String dbname)
	{
		myDbEnvironment = new Environment(new File(GlobalConstants.PREFIX_DB_PATH), envConfig);
		dbConfig = new DatabaseConfig();
		dbConfig.setAllowCreate(true);
		myDatabase = myDbEnvironment.openDatabase(null, dbname, dbConfig);
		return myDatabase;
		
	}
	public void createIndex(String filename, int colIndex )
	{
		BufferedReader br=null;
		try {
			br= new BufferedReader(new FileReader(GlobalConstants.PREFIX_DATA_PATH+GlobalConstants.SLASH+filename+GlobalConstants.DAT_SUFFIX));
			String line=null;
			String[] cols =null;
			while((line = br.readLine())!=null)
			{
				cols=line.split("|");
				DatabaseEntry theKey = new DatabaseEntry(cols[colIndex].getBytes("UTF-8"));
				DatabaseEntry theData = new DatabaseEntry(line.getBytes("UTF-8"));
				myDatabase.put(null, theKey, theData);
			}
		}
		catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
