package edu.buffalo.cse562.operators;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import edu.buffalo.cse562.beans.Schema;
import edu.buffalo.cse562.beans.Tuple;
import edu.buffalo.cse562.eval.Evaluator;
import edu.buffalo.cse562.interfaces.Operator;
import edu.buffalo.cse562.sqlparser.ColDetails;

public class ExtProjOperator extends BaseOperator implements Operator {

	Operator input;
	String Alias;
	List<SelectItem> items;

	public ExtProjOperator(Operator input, Schema schema, List<SelectItem> selectItems,String alias)
	{ 
		this.input=input;
		this.schema=schema;
		this.items=selectItems;
		this.Alias=alias;
	}
	@Override
	public Tuple getNext() {
		Tuple tuple;
		List<LeafValue>  list = new ArrayList<LeafValue>();
		Tuple retTuple = new Tuple();
		
		tuple=input.getNext();
		if(null==tuple || tuple.tupleItems==null)
		{
			return null;
		}
		for(SelectItem sel : this.items)
		{
			if(sel instanceof AllColumns)
				return tuple;
			else if(sel instanceof SelectExpressionItem){
				Expression exp = ((SelectExpressionItem)sel).getExpression();
				Evaluator eval = new Evaluator(schema, tuple.tupleItems);
				try {
					list.add(eval.eval(exp));
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		retTuple.tupleItems = new LeafValue[list.size()];
		int count = 0;
		for(LeafValue val:list){
			retTuple.tupleItems[count++]=val;
		}

		return retTuple;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub

	}

}
