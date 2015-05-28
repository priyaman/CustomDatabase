package edu.buffalo.cse562.sqlparser;

import java.util.List;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import edu.buffalo.cse562.beans.Schema;
import edu.buffalo.cse562.utils.ExternalSortUtil;

public class CreateTableParser {

	public CreateTableParser() {
		// TODO Auto-generated constructor stub
	}
	
	public Schema parseCreateTable(CreateTable stmt){
		Table tableDef = stmt.getTable();
	
		Schema schema = new Schema();
		schema.table = tableDef;
		List<ColumnDefinition> colDefs = stmt.getColumnDefinitions();
		int colCtr = 0;
		for(ColumnDefinition colDef:colDefs){
			colDef.setColumnName(colDef.getColumnName().toUpperCase());
			Column col = new Column(tableDef, colDef.getColumnName().toUpperCase());
			
			schema.putColDetails(col.getWholeColumnName().toUpperCase(), schema.getColDetails( col, colDef));
			schema.colIdxMap.put(col.getWholeColumnName().toUpperCase(), colCtr);
			colCtr++;
		}
	//	ExternalSortUtil.getTupleSize(schema);
		return schema;
	}

}
