package edu.buffalo.cse562.operators;

import java.sql.SQLException;

import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import edu.buffalo.cse562.beans.Schema;
import edu.buffalo.cse562.beans.Tuple;
import edu.buffalo.cse562.eval.Evaluator;
import edu.buffalo.cse562.interfaces.Operator;

public class SelectOperator extends BaseOperator implements Operator {
	Operator input;
	Expression exp;
	
	public SelectOperator(Operator input, Expression exp, Schema schema) {
		this.input = input;
 		this.exp = exp;
 		this.schema = schema;
 	}

	@Override
	public Tuple getNext() {
Tuple tuple;
		Tuple retTuple = null;//new Tuple();
		
		do{
			tuple = input.getNext();
			if(tuple==null || tuple.tupleItems==null)
				return null;
			else{
				Evaluator lt = new Evaluator(schema, tuple.tupleItems);
				try {
					BooleanValue result = (BooleanValue) lt.eval(exp);
					if(result.getValue())
						retTuple = tuple;
				} catch (SQLException e) {
					System.err.println("Error in Eval");
					e.printStackTrace();	
				}
			}
		}while(retTuple==null || tuple.tupleItems==null);
	
		return retTuple;
	
	}

	@Override
	public void reset() {
		this.input.reset();
		
	}

}
