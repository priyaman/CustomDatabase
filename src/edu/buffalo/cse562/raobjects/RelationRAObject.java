package edu.buffalo.cse562.raobjects;

import edu.buffalo.cse562.globals.GlobalConstants.RAOperator;

public class RelationRAObject extends BaseRAObject{

	public String tablename;
	public String alias;
	
	public RelationRAObject() {
		// TODO Auto-generated constructor stub
	}
	public RelationRAObject(BaseRAObject parent) {
		this.parent = parent;
	}
	public RelationRAObject(BaseRAObject parent, String tablename) {
		this.parent = parent;
		this.tablename = tablename;
		this.operator = RAOperator.RELATION;
	}

}
