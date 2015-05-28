package edu.buffalo.cse562.operators;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.dbi.DbConfigManager;

import edu.buffalo.cse562.beans.Schema;
import edu.buffalo.cse562.beans.Tuple;
import edu.buffalo.cse562.globals.GlobalConstants;
import edu.buffalo.cse562.interfaces.Operator;
import edu.buffalo.cse562.sqlparser.ColDetails;
import edu.buffalo.cse562.utils.Utilities;

public class IndexReadOperator extends BaseOperator implements Operator{

	Operator leftOperator;
	List<ColumnDefinition> colNames=null;
	Schema leftSchema = null;
	Schema rightSchema = null;
	Expression joinExp = null;
	Tuple cachedLeftTuple = null;
	Cursor rightCursor = null;
	List<Tuple> cachedTuples = null;
	Iterator<Tuple> cacheIterator = null;
	
	public IndexReadOperator(Schema leftSchema, Operator leftOperator, Schema rightSchema, Expression joinExp) {
		this.leftOperator = leftOperator;
		this.leftSchema = leftSchema;
		this.rightSchema = rightSchema;
		this.joinExp = joinExp;
		boolean isPrimary = false;
		String[] tabColName = ((BinaryExpression)joinExp).getRightExpression().toString().toUpperCase().split("\\.");
		if(rightSchema.colIdxMap.get(tabColName[1])==0)
			isPrimary = true;
		colNames = getColDefs(schema);
		DatabaseConfig cfg = new DatabaseConfig();
		cfg.setReadOnly(true);
		EnvironmentConfig envCfg = new EnvironmentConfig();
		envCfg.setAllowCreate(false);
		
		GlobalConstants.indexEnv = new Environment(new File(GlobalConstants.PREFIX_DB_PATH), envCfg);
		Database rightPrimaryIdx = GlobalConstants.indexEnv.openDatabase(null, tabColName[0], cfg);
		SecondaryDatabase rightSeconIdx = null;
		if(!isPrimary){
			SecondaryConfig cfg2 = new SecondaryConfig();
			cfg2.setReadOnly(true);
			rightSeconIdx = GlobalConstants.indexEnv.openSecondaryDatabase(null, tabColName[1], rightPrimaryIdx, cfg2);
			rightCursor = rightSeconIdx.openSecondaryCursor(null, null);
		}else{
			
			rightCursor = rightPrimaryIdx.openCursor(null, null);
		}
		
	}
	
	private List<ColumnDefinition> getColDefs(Schema schema)
	{
		List<ColumnDefinition> colDefs = new ArrayList<ColumnDefinition>();

		ColumnDefinition[] arr = new ColumnDefinition[schema.ColumnMap.size()];
		Iterator it = schema.ColumnMap.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, ColDetails> pair = (Entry<String, ColDetails>) it
					.next();

			int a = schema.colIdxMap.get(pair.getKey().toUpperCase());
			arr[a] = (pair.getValue().colDef);

		}

		for (int i = 0; i < arr.length; i++) {
			colDefs.add(arr[i]);
		}

		return colDefs;
	}
	
	
	
	@Override
	public Tuple getNext() {
		/*String leftKey;
		if(cachedLeftTuple==null){
			cachedLeftTuple = leftOperator.getNext();
			leftKey = Utilities.leafValuetoString(cachedLeftTuple.tupleItems[leftSchema.colIdxMap.get(key)]);
		}
		DatabaseEntry key = new DatabaseEntry(leftKey.getBytes("UTF-8"));
		DatabaseEntry data = new DatabaseEntry();
		OperationStatus retVal = rightCursor.getSearchKey(key, data, LockMode.DEFAULT);
		while(retVal==OperationStatus.SUCCESS){
			
		}*/
		
		if(!cacheIterator.hasNext()){
			cachedTuples = new ArrayList<Tuple>();
			Tuple leftTuple = leftOperator.getNext();
			String leftKey = Utilities.leafValuetoString(leftTuple.tupleItems[leftSchema.colIdxMap.get(key)]);
			DatabaseEntry key = new DatabaseEntry(leftKey.getBytes("UTF-8"));
			DatabaseEntry data = new DatabaseEntry();
			OperationStatus retVal = rightCursor.getSearchKey(key, data, LockMode.DEFAULT);
			while(retVal==OperationStatus.SUCCESS){
				 //String keyString = new String(key.getData(), "UTF-8");
		         String dataString = new String(data.getData(), "UTF-8");
		 		 String[] data_champ = null;
				 if(dataString!=null)
					 data_champ = dataString.split("\\|");
				 else
					return null;
				Tuple retTuple = new Tuple();
				retTuple.tupleItems = new LeafValue[data_champ.length];


				for (int i = 0; i < data_champ.length; i++) {
					String datatype = colNames.get(i).getColDataType().getDataType();
					if (datatype.equalsIgnoreCase(GlobalConstants.INTEGER_TYPE)
							|| datatype.equalsIgnoreCase(GlobalConstants.LONG_TYPE)) {
						retTuple.tupleItems[i] = new LongValue(data_champ[i]);
						//retTuple.tupleItems[i] = new DoubleValue(data[i]);
					} else if (datatype.equalsIgnoreCase(GlobalConstants.STRING_TYPE)
							|| datatype.equalsIgnoreCase(GlobalConstants.VARCHAR_TYPE)
							|| datatype.equalsIgnoreCase(GlobalConstants.CHAR_TYPE)) {
						retTuple.tupleItems[i] = new StringValue(" "+data_champ[i]+ " ");

						// ((StringValue)retVal[i]).setValue(((StringValue)retVal[i]).toString().replaceFirst("'",
						// ""));
						// retVal[i] = new StringValue(data[i].substring(1,
						// data[i].length()-1));
					} else if (datatype.equalsIgnoreCase(GlobalConstants.DATE_TYPE)) {
						retTuple.tupleItems[i] = new DateValue("'" + data_champ[i] + "'");

					} else if (datatype.equalsIgnoreCase(GlobalConstants.DOUBLE_TYPE)) {
						retTuple.tupleItems[i] = new DoubleValue(data_champ[i]);
					} else{
						System.out.println("Invalid datatype" + datatype.toString());
					}
				}
				 cachedTuples.add(mergeTuple(leftTuple, retTuple));
		         retVal = rightCursor.getNextDup(key, data, LockMode.DEFAULT);
			}	
			cacheIterator = cachedTuples.iterator();
			return cacheIterator.next();
			
		}else{
			return cacheIterator.next();
		}
		
		
		return null;
	}
	
	private Tuple mergeTuple(Tuple leftTuple, Tuple rightTuple){
		Tuple t = new Tuple();
		t.tupleItems = new LeafValue[leftTuple.tupleItems.length + rightTuple.tupleItems.length];
		for(int i=0;i<leftTuple.tupleItems.length;i++)
			t.tupleItems[i] = leftTuple.tupleItems[i];
		for(int i=leftTuple.tupleItems.length-1;i<t.tupleItems.length;i++)
			t.tupleItems[i] = rightTuple.tupleItems[i];
		return t;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		
	}

}
