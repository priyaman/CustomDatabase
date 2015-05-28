package edu.buffalo.cse562.utils;

import java.util.Comparator;

import edu.buffalo.cse562.beans.Tuple;

public class StringComparator implements Comparator<Tuple> {
	 int columnIndex;
	 public StringComparator(int index){
		 this.columnIndex = index;
	 }
		@Override
		public int compare(Tuple o1, Tuple o2) {
            if(o1.tupleItems[columnIndex].toString().compareTo(o2.tupleItems[columnIndex].toString())>1)
            {
                return 1;
            }else{
            	return -1;
            }
		}
}
