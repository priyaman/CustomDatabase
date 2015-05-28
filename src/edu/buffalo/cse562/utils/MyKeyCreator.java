package edu.buffalo.cse562.utils;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;

public class MyKeyCreator implements SecondaryKeyCreator {

	TupleBinding theBinding;
	int colIndex;
	public MyKeyCreator(TupleBinding theBinding1) {
		colIndex=3;
		theBinding = theBinding1;
}
	@Override
	public boolean createSecondaryKey(SecondaryDatabase arg0,
			DatabaseEntry arg1, DatabaseEntry arg2, DatabaseEntry arg3) {
		
		String data = (String) theBinding.entryToObject(arg2);
		String result = data.split("\\|")[colIndex]; 
		theBinding.objectToEntry(result, arg3);
		
		// TODO Auto-generated method stub
		return false;
	}

}
