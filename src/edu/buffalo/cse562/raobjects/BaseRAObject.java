package edu.buffalo.cse562.raobjects;

import edu.buffalo.cse562.beans.Schema;
import edu.buffalo.cse562.globals.GlobalConstants.RAOperator;
import edu.buffalo.cse562.interfaces.Operator;

public class BaseRAObject {

	public RAOperator operator;
	public Operator operation;
	public Schema inSchema;
	public Schema outSchema;
	public BaseRAObject parent;
	public BaseRAObject leftChild;
	public BaseRAObject rightChild;
	//public Schema operatorSchema;
	
	public BaseRAObject() {
		this.operator = null;
		parent =null;
		leftChild = null;
		rightChild = null;
	}
	
	public BaseRAObject(RAOperator operator) {
		this.operator = operator;
		parent =null;
		leftChild = null;
		rightChild = null;
	}
	
	public BaseRAObject(BaseRAObject parent){
		this.parent = parent;
		leftChild = null;
		rightChild = null;
	}
	
	public BaseRAObject(BaseRAObject parent, BaseRAObject child){
		this.parent = parent;
		leftChild = child;
		rightChild = null;
	}
	
	public BaseRAObject(BaseRAObject parent, BaseRAObject leftChild,
			BaseRAObject rightChild){
		this.parent = parent;
		this.leftChild = leftChild;
		this.rightChild = rightChild;
	}

}
