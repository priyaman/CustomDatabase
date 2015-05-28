package edu.buffalo.cse562.utils;

import java.util.Comparator;

import net.sf.jsqlparser.expression.DateValue;
import edu.buffalo.cse562.beans.Tuple;

public class DateComparator implements Comparator<Tuple>
{
 
 int columnIndex;
 public DateComparator(int index){
	 this.columnIndex = index;
 }
	@Override
	public int compare(Tuple o1, Tuple o2) {
        if(((DateValue)o1.tupleItems[columnIndex]).getValue().compareTo(((DateValue)o2.tupleItems[columnIndex]).getValue()) >1)
        {
            return 1;
        }else{
        	return -1;
        }
	}
}

