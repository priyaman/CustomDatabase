package edu.buffalo.cse562.beans;

import java.util.HashMap;
import java.util.Map;

import edu.buffalo.cse562.sqlparser.ColDetails;
import edu.buffalo.cse562.utils.ExternalSortUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class Schema {

	/**
	 * Inner class to keep column Index in tuple 
	 * Column to keep the column details
	 */
	
	
	public Table table; //Will contain tablename and alias
	public Map<String,Integer> colIdxMap; //Map of TableName.ColumnName to Column Index
	public Map<String, ColDetails> ColumnMap; //Map of TableName.ColumnName to Columns
	public long tupleSize;
	
	public Schema() {
		colIdxMap = new HashMap<String, Integer>();
		ColumnMap = new HashMap<String, ColDetails>();
		table = new Table();
		ExternalSortUtil.getTupleSize(this);
	}

	public Schema(Table table, Map<String, ColDetails> ColumnMap,
			Map<String, Integer> colIdxMap) {
		this.table = table;
		this.ColumnMap = ColumnMap;
		this.colIdxMap = colIdxMap;
		ExternalSortUtil.getTupleSize(this);
	}
	
	//Utility fn to get an instance of the inner class.
	public ColDetails getColDetails(Column colDetails, ColumnDefinition colDef){
		ColDetails colDet = new ColDetails();
		colDet.colDetails = colDetails;
		colDet.colDef = colDef;
		return colDet;
	}
	
	public void putColDetails(String wholeColName, ColDetails colDet){
		ColumnMap.put(wholeColName, colDet);
	}

}
