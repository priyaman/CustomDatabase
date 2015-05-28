package edu.buffalo.cse562.utils;

import edu.buffalo.cse562.globals.GlobalConstants;
import edu.buffalo.cse562.raobjects.AggregateRAObject;
import edu.buffalo.cse562.raobjects.BaseRAObject;
import edu.buffalo.cse562.raobjects.ExProjectRAObject;
import edu.buffalo.cse562.raobjects.JoinRAObject;
import edu.buffalo.cse562.raobjects.RelationRAObject;
import edu.buffalo.cse562.raobjects.WhereRAObject;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;

public class Utilities {
	
	public static String leafValuetoString(LeafValue leaf){
	String returnString = null;
	try{
		if(leaf instanceof LongValue){
			returnString = String.valueOf(leaf.toLong());
		}
		else if(leaf instanceof DoubleValue){
			returnString = String.valueOf(leaf.toDouble());
		}
		else if(leaf instanceof DateValue){
			returnString = leaf.toString();
		}
		else if(leaf instanceof StringValue){
			returnString = leaf.toString();
		}
		else{
			System.out.println("You forgot a datatype.");
		}
	}
	catch(Exception e){
		System.out.println("Error in leafvalueToString.");
		}
	return returnString;
	}
	
	public static String getRandomString(){
		int charactersLength = GlobalConstants.characters.length();
		StringBuffer buffer = new StringBuffer();

		for (int i = 0; i < 5; i++) {
			double index = Math.random() * charactersLength;
			buffer.append(GlobalConstants.characters.charAt((int) index));
		}
		return buffer.toString();
	}
	
	public static void printRATree(BaseRAObject head){
		if(head==null)
			return;
	/*	if(head.leftChild!=null)
			printRATree(head.leftChild);
		if(head.rightChild!=null)
			printRATree(head.rightChild);*/
	//	System.out.print(head.operator + "  ");
		if(head instanceof JoinRAObject){
			System.out.print("JT:" + ((JoinRAObject)head).joinType);
			System.out.print(" JoinExp: " + ((JoinRAObject)head).onExp);
		}
		if(head instanceof RelationRAObject)
			System.out.print(" Relation:" + ((RelationRAObject)head).tablename);
		if(head instanceof WhereRAObject)
			System.out.print(" selction:" + ((WhereRAObject)head).exp);
		if(head instanceof ExProjectRAObject)
			System.out.print(" project:" + ((ExProjectRAObject)head).items.toString());
		if(head instanceof AggregateRAObject)
			System.out.print(" aggregate:" + ((AggregateRAObject)head).items.toString());
		System.out.println();
	}

}
