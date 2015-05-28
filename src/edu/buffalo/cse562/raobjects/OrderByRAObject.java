package edu.buffalo.cse562.raobjects;

import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.OrderByElement;
import edu.buffalo.cse562.beans.Schema;

public class OrderByRAObject extends BaseRAObject {
	public List<OrderByElement> orderByItems;
	public OrderByRAObject()
	{
		
	}
	public void createSchema(Schema schema)
	{
		this.inSchema=schema;
		this.outSchema=schema;
	}
}
