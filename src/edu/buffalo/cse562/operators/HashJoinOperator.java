package edu.buffalo.cse562.operators;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import edu.buffalo.cse562.beans.Schema;
import edu.buffalo.cse562.beans.Tuple;
import edu.buffalo.cse562.eval.Evaluator;
import edu.buffalo.cse562.interfaces.Operator;
import edu.buffalo.cse562.sqlparser.ColDetails;
import edu.buffalo.cse562.utils.Utilities;

public class HashJoinOperator extends BaseOperator implements Operator {

	private Operator leftOperator;
	private Operator rightOperator;
	private Schema leftSchema;
	private Schema rightSchema;
	private ColDetails leftCol;
	private ColDetails rightCol;
	private Schema newSchema;
	//HashMap
	private HashMap<String, List<Tuple>> leftHash;
	//leftReturner
	private Iterator<Tuple> leftIterator = null;
	//Right Tuple to return
	Tuple rightTuple = null;
	//Evalator
	Evaluator eval;
	//JoinExpression
	Expression joinExp;
	public HashJoinOperator(Operator leftOperator, Operator rightOperator, Schema leftSchema, Schema rightSchema, Schema newSchema, ColDetails leftCol, ColDetails rightCol, Expression joinExp){
		this.leftOperator = leftOperator;
		this.rightOperator = rightOperator;
		this.leftSchema = leftSchema;
		this.rightSchema = rightSchema;
		this.leftCol = leftCol;
		this.rightCol = rightCol;
		this.joinExp = joinExp;
		this.initialize();
		/*this.newSchema = new Schema();
		this.newSchema.ColumnMap.putAll(leftSchema.ColumnMap);
		this.newSchema.ColumnMap.putAll(rightSchema.ColumnMap);
		this.newSchema.colIdxMap.putAll(leftSchema.colIdxMap);
		int index1= leftSchema.colIdxMap.size();
		Iterator it = this.rightSchema.colIdxMap.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry<String,Integer> pair = (Map.Entry<String,Integer>)it.next();
	        	int index2 = index1+pair.getValue();
	        		this.newSchema.colIdxMap.put(pair.getKey().toUpperCase(), index2);
	    }*/
	    schema = newSchema;
	}
	
	private void initialize(){
	//	System.out.println("Entering Initialize for:" + ((BaseOperator)leftOperator).schema.colIdxMap.toString());
	
		leftHash = new HashMap<String, List<Tuple>>();
		Tuple leftTuple = new Tuple();
		List<Tuple> currList = null;
	    Tuple currLeaves = null;
		while((currLeaves = (leftOperator.getNext()))!=null && currLeaves.tupleItems!=null){
			leftTuple = currLeaves;
			currList = leftHash.get(Utilities.leafValuetoString(leftTuple.tupleItems[leftSchema.colIdxMap.get(leftCol.colDetails.getWholeColumnName())]));
			if(currList==null){
				currList = new ArrayList<Tuple>();
			}
			currList.add(leftTuple);
			leftHash.put(Utilities.leafValuetoString(leftTuple.tupleItems[leftSchema.colIdxMap.get(leftCol.colDetails.getWholeColumnName())]), currList);	
		}
	//	System.out.println("Exiting Initialize for:" + ((BaseOperator)leftOperator).schema.table.getWholeTableName() + " leftHash size:" + leftHash.size());
	}
	
	private Tuple mergeTuple(Tuple leftTuple, Tuple rightTuple){
		Tuple retTuple = new Tuple();
		retTuple.tupleItems = new LeafValue[leftTuple.tupleItems.length + rightTuple.tupleItems.length];
		int ctr = 0;
		for(int i=0;i<leftTuple.tupleItems.length;i++)
			retTuple.tupleItems[ctr++] = leftTuple.tupleItems[i];
		for(int i=0;i<rightTuple.tupleItems.length;i++)
			retTuple.tupleItems[ctr++] = rightTuple.tupleItems[i];
		return retTuple;
	}
	
	
	
	/*@Override
	public LeafValue[] getNext() {
		LeafValue[] returnTup = null;
		if(leftReturner==null && rightTuple==null){
			rightTuple = new Tuple(rightOperator.getNext());
			if(rightTuple==null)
				return null;
		}
		do{
			Tuple leftTuple = null;
			if(leftReturner!=null && leftReturner.hasNext()){
				leftTuple = leftReturner.next();
			}else{
				List<Tuple> currList = null;
				do{
					if(rightTuple==null){
						LeafValue[] rightLeafs = rightOperator.getNext();
						if(rightLeafs==null)
							return null;
						rightTuple = new Tuple(rightLeafs);
					}
				
					currList = leftHash.get(Utilities.leafValuetoString(rightTuple.tupleItems[rightSchema.colIdxMap.get(rightCol.colDetails.getWholeColumnName())]));
					if(currList!=null){
						leftReturner= currList.iterator();
						leftTuple = leftReturner.next();
					}else{
						rightTuple=null;
					}
				}while(currList==null);
			}	
			returnTup= mergeTuple(leftTuple, rightTuple);
			//Eval joinexp
			eval = new Evaluator(newSchema, returnTup);

			BooleanValue result = null;
			try {
				result = (BooleanValue)eval.eval(joinExp);
			} catch (SQLException e) {
				System.out.println("Archana's code doesnt work. Hash Join Eval Error");
					e.printStackTrace();
			}
			if(!result.getValue())
					returnTup=null;
			else{
				if(leftReturner.hasNext())
					leftTuple = leftReturner.next();
				else{
					LeafValue[] rightLeafs = rightOperator.getNext();
					if(rightLeafs==null)
						return null;
					rightTuple = new Tuple(rightLeafs);
					leftReturner = null;
					leftTuple = null;
				}
			}
		}while(returnTup==null);
		
		return returnTup;
	}
*/
	@Override
	public Tuple getNext(){
		Tuple leftTuple = null;
		if(leftIterator==null || !leftIterator.hasNext() ){
			//GET RIGHT TUPLE
			Tuple rightLeaves = rightOperator.getNext();
			if(rightLeaves==null || rightLeaves.tupleItems==null)
				return null;
			rightTuple = rightLeaves;
			//LOOKUP MAP
			List<Tuple> currList = null;
			do{
				currList = leftHash.get(Utilities.leafValuetoString(rightTuple.tupleItems[rightSchema.colIdxMap.get(rightCol.colDetails.getWholeColumnName())]));
				if(currList==null){
					rightLeaves = rightOperator.getNext();
					if(rightLeaves==null || rightLeaves.tupleItems==null)
						return null;
					rightTuple = rightLeaves;
				}
			}while(currList==null);
			leftIterator = currList.iterator();
		}
		leftTuple = leftIterator.next();
		//MERGE TUPLES
		Tuple mergedTuple = mergeTuple(leftTuple, rightTuple);
		
		return mergedTuple; 
		
	}
	@Override
	public void reset() {
		// TODO Auto-generated method stub
		
	}

}
