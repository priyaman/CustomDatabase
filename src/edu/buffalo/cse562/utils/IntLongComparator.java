package edu.buffalo.cse562.utils;

import java.util.Comparator;

import net.sf.jsqlparser.expression.LongValue;
import edu.buffalo.cse562.beans.Tuple;

public class IntLongComparator implements Comparator<Tuple>
{
 
 int columnIndex;
 public IntLongComparator(int index){
	 this.columnIndex = index;
 }
	@Override
	public int compare(Tuple o1, Tuple o2) {
        if(((LongValue)o1.tupleItems[columnIndex]).toLong() > ((LongValue)o2.tupleItems[columnIndex]).toLong())
        {
            return 1;
        }else{
        	return -1;
        }
	}
}