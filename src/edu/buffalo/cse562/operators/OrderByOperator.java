package edu.buffalo.cse562.operators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import edu.buffalo.cse562.beans.Schema;
import edu.buffalo.cse562.beans.Tuple;
import edu.buffalo.cse562.globals.GlobalConstants;
import edu.buffalo.cse562.interfaces.Operator;
import edu.buffalo.cse562.utils.orderClass;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;

public class OrderByOperator extends BaseOperator implements Operator {
	List<OrderByElement> orderbyItems;
	Operator input;
	
	boolean called;
	orderClass[] order;
	List<Tuple> toBeOrdered;
	public OrderByOperator(Operator input,List<OrderByElement> orderbyItems, Schema schema)
	{
		toBeOrdered = new ArrayList<Tuple>();
		called=false;
		 
		this.input=input;
		this.orderbyItems=orderbyItems;
		 order= new orderClass[this.orderbyItems.size()];
		this.schema=schema;
		for(int i=0;i<this.orderbyItems.size();i++)
		{
			order[i]=new orderClass();
			Column col = (Column)this.orderbyItems.get(i).getExpression(); 
			
			if(this.schema.colIdxMap.get(col.getWholeColumnName().toUpperCase())!=null)
			order[i].index=this.schema.colIdxMap.get(col.getWholeColumnName().toUpperCase());
			else
			{
				Set<String> s = this.schema.colIdxMap.keySet();
				Iterator<String> iter = s.iterator();
				while (iter.hasNext()) {
					String str = iter.next();
				    if(str.contains(".") && str.split("\\.", 2)[1].equalsIgnoreCase(col.getWholeColumnName()))
				    	order[i].index=this.schema.colIdxMap.get(str);
				}
			}
			order[i].isAsc=this.orderbyItems.get(i).isAsc();
			
		}
		
	}
	@Override
	public Tuple getNext() {
		if(!called)
		{
		Tuple tuple;
		
		int count=0;
		while(true)
		{ 
			
			
			tuple=input.getNext();
			if( tuple==null || tuple.tupleItems==null)
				break;
				toBeOrdered.add(tuple);
				
			//((Tuple)toBeOrdered.get(0)).
			
			
		}
		
		Tuple.order=order;
		System.err.println();
		
		
		Collections.sort(toBeOrdered, new TupleComparator());
		called=true;
		}
		Tuple rettuple = null;
		if(!toBeOrdered.isEmpty())
		{rettuple = toBeOrdered.get(0);
		toBeOrdered.remove(0);
		}
		return rettuple;
	}
	class TupleComparator implements Comparator<Tuple> {
		
		public int compareLeafAsc(LeafValue l1, LeafValue l2)
		{
			
			if(l1 instanceof DateValue)
			{
				if(((DateValue)l1).getValue().compareTo(((DateValue)l2).getValue())>=1)
		        {
		            return 1;
		        }
				else if(((DateValue)l1).getValue().compareTo(((DateValue)l2).getValue())==0)
				{
					return 0;
				}
				else{
		        	return -1;
		        }
			}
			else if(l1 instanceof DoubleValue)
			{
				if(((DoubleValue)l1).getValue()>((DoubleValue)l2).getValue())
		        {
		            return 1;
		        }
				else if(((DoubleValue)l1).getValue()==((DoubleValue)l2).getValue())
				{
					return 0;
				}
				else{
		        	return -1;
		        }
			}
			else if(l1 instanceof LongValue)
			{
				if(((LongValue)l1).getValue()>((LongValue)l2).getValue())
		        {
		            return 1;
		        }
				else if(((LongValue)l1).getValue()==((LongValue)l2).getValue())
				{
					return 0;
				}
				else{
		        	return -1;
		        }
			}
			else if(l1 instanceof StringValue)
			{
				if(((StringValue)l1).getValue().compareTo(((StringValue)l2).getValue())>=1)
		        {
		            return 1;
		        }
				else if(((StringValue)l1).getValue().compareTo(((StringValue)l2).getValue())==0)
				{
					return 0;
				}
				else{
		        	return -1;
		        }
			}
			return 0;
		}
		
		@Override
		public int compare(Tuple o1, Tuple o2) {
			// TODO Auto-generated method stub
			for(int l=0;l<order.length;l++)
			{	int i = order[l].index;
				int res;
					
					res=compareLeafAsc(o1.tupleItems[i],o2.tupleItems[i]);
					if(order[l].isAsc!=true)
					{
					res=res*-1;
				}
					if(res!=0)
					return res;
			
			}
			return 0;
		}

		
	}
	 
	@Override
	public void reset() {
		// TODO Auto-generated method stub
		
	}
	
}
