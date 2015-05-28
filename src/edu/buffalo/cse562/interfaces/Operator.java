package edu.buffalo.cse562.interfaces;

import edu.buffalo.cse562.beans.Tuple;

public interface Operator {
	public Tuple getNext();
	public void reset();
	
}
