package edu.buffalo.cse562.raobjects;

import net.sf.jsqlparser.expression.Expression;
import edu.buffalo.cse562.beans.Schema;
import edu.buffalo.cse562.globals.GlobalConstants.RAOperator;

public class WhereRAObject extends BaseRAObject {

	public Expression exp;
	public WhereRAObject(RAOperator operator) {
		super(operator);
		// TODO Auto-generated constructor stub
	}

	public WhereRAObject(BaseRAObject parent) {
		super(parent);
		// TODO Auto-generated constructor stub
	}

	public WhereRAObject(BaseRAObject parent, BaseRAObject child) {
		super(parent, child);
		// TODO Auto-generated constructor stub
	}

	public WhereRAObject(BaseRAObject parent, BaseRAObject leftChild,
			BaseRAObject rightChild) {
		super(parent, leftChild, rightChild);
		// TODO Auto-generated constructor stub
	}
	public void createSchema(Schema schema)
	{
		this.inSchema=schema;
		this.outSchema=schema;
	}
	

}
