package edu.buffalo.cse562.utils;

import java.util.Comparator;

import net.sf.jsqlparser.expression.DoubleValue;
import edu.buffalo.cse562.beans.Tuple;

public class DoubleComparator implements Comparator<Tuple>
{
 
 int columnIndex;
 public DoubleComparator(int index){
	 this.columnIndex = index;
 }
	@Override
	public int compare(Tuple o1, Tuple o2) {
        if(((DoubleValue)o1.tupleItems[columnIndex]).toDouble() > ((DoubleValue)o2.tupleItems[columnIndex]).toDouble())
        {
            return 1;
        }else{
        	return -1;
        }
	}
}
