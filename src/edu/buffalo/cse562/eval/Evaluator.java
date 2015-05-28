package edu.buffalo.cse562.eval;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.schema.Column;
import edu.buffalo.cse562.Eval;
import edu.buffalo.cse562.beans.Schema;
import edu.buffalo.cse562.globals.GlobalConstants;



public class Evaluator extends Eval {

	Schema schema1;
	LeafValue[] tuple1;
	public Evaluator(Schema schema1, LeafValue[] tuple) {
		this.schema1 = schema1;
		this.tuple1 = tuple;
	}

	@Override
	public LeafValue eval(Column col) throws SQLException {
		String wholeName="";
		int index=0;
		if(col.getWholeColumnName().contains(GlobalConstants.DOT))
		{
			wholeName=col.getWholeColumnName();
		}
		else
		{
			if(this.schema1.table==null || this.schema1.table.getName()==null)
				
				wholeName=col.getWholeColumnName();
			else
			 wholeName=this.schema1.table.getName()+GlobalConstants.DOT+col.getColumnName();
		}
		
	    index = this.schema1.colIdxMap.get(wholeName.toUpperCase());
		return this.tuple1[index];
			}

}
