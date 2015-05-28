package edu.buffalo.cse562.operators;

import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.statement.select.Limit;
import edu.buffalo.cse562.beans.Tuple;
import edu.buffalo.cse562.globals.GlobalConstants;
import edu.buffalo.cse562.interfaces.Operator;

public class PrintOperator extends BaseOperator implements Operator {

	Operator input;
	Limit limit;
	public PrintOperator(Operator input,Limit limit)
	{
		this.input=input;
		this.limit=limit;
	
	}
	@Override
	public Tuple getNext() {
		// TODO Auto-generated method stub
		Tuple tuple;
		int count=0;

		do
		{ 
			count++;
			
			tuple=input.getNext();
			if(tuple!=null && tuple.tupleItems!=null)
			{
				for(int i=0;i<tuple.tupleItems.length-1;i++)
					
				{ if(tuple.tupleItems[i] instanceof StringValue)
					//System.out.print(((StringValue)tuple.tupleItems[i]).getNotExcapedValue()+"|");
					System.out.print(((StringValue)tuple.tupleItems[i]).toString().replaceAll("'", "")+GlobalConstants.PIPE);
				else	
				System.out.print(tuple.tupleItems[i]+GlobalConstants.PIPE);
				}
				 if(tuple.tupleItems[tuple.tupleItems.length-1] instanceof StringValue)
					//System.out.print(((StringValue)tuple.tupleItems[tuple.tupleItems.length-1]).getNotExcapedValue());
					System.out.print(((StringValue)tuple.tupleItems[tuple.tupleItems.length-1]).toString().replaceAll("'", ""));
				else	
				System.out.print(tuple.tupleItems[tuple.tupleItems.length-1]);
				
				
				System.out.println();
			}
			if(limit!=null && count>=this.limit.getRowCount())
				break;
		}while( tuple!=null && tuple.tupleItems!=null);
		return null;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		
	}
	

}
