package edu.buffalo.cse562.beans;

import java.util.List;

import edu.buffalo.cse562.utils.orderClass;
import net.sf.jsqlparser.expression.LeafValue;

//public class Tuple  implements Comparator<Tuple>{
public class Tuple {
	public LeafValue[] tupleItems; 
	
	public static orderClass[] order;
	
	//Data
//	public String schemaKey; //Will point to schema of the tuple
	
	public Tuple() {
		
	}

	public Tuple(LeafValue[] tupleItems, String schemaKey) {
		super();
		this.tupleItems = tupleItems;
	//	this.schemaKey = schemaKey;
	}
	
	public Tuple(LeafValue[] tupleItems){
		this.tupleItems = tupleItems;
	}


	

}
