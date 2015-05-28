package edu.buffalo.cse562.raobjects;

import java.util.Iterator;
import java.util.Map;

import edu.buffalo.cse562.beans.Schema;
import edu.buffalo.cse562.globals.GlobalConstants;
import edu.buffalo.cse562.globals.GlobalConstants.JoinTypes;
import edu.buffalo.cse562.globals.GlobalConstants.RAOperator;
import edu.buffalo.cse562.interfaces.Operator;
import net.sf.jsqlparser.expression.Expression;

public class JoinRAObject extends BaseRAObject {
	
	public Expression onExp;
	public Operator ListOperator;
	public Schema leftSchema;
	public Schema rightSchema;
	public JoinTypes joinType = JoinTypes.CROSS;
	
	public JoinRAObject(BaseRAObject parent) {
		super(parent);
		this.operator=RAOperator.JOIN;
		// TODO Auto-generated constructor stub
	}
	public JoinRAObject() {
		super();
		this.operator=RAOperator.JOIN;
		// TODO Auto-generated constructor stub
	}
	
	public void setJoinType(){
		if(GlobalConstants.HAS_SWAP)
			this.joinType = JoinTypes.MERGE;
		else
			this.joinType = JoinTypes.HASH;
	}
	
	public void createSchema(Schema left, Schema right)
	{
		
		this.leftSchema = left;
		this.rightSchema = right;
		this.outSchema = new Schema();
		this.outSchema.ColumnMap.putAll(leftSchema.ColumnMap);
		this.outSchema.ColumnMap.putAll(rightSchema.ColumnMap);
		this.outSchema.colIdxMap.putAll(leftSchema.colIdxMap);
		int index1=leftSchema.colIdxMap.size();
		Iterator it = this.rightSchema.colIdxMap.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry<String,Integer> pair = (Map.Entry<String,Integer>)it.next();
	        	int index2 = index1+pair.getValue();
	        		this.outSchema.colIdxMap.put(pair.getKey().toUpperCase(), index2);
	    }	
	}
	

}
