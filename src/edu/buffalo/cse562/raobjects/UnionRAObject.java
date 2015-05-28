package edu.buffalo.cse562.raobjects;

import java.util.ArrayList;
import java.util.List;

public class UnionRAObject extends BaseRAObject {
	public List<BaseRAObject> statementHeads = null;
	public UnionRAObject() {
		statementHeads = new ArrayList<BaseRAObject>();
	}
	public UnionRAObject(List<BaseRAObject> statmenetHeads) {
		this.statementHeads = statementHeads;
	}

}
