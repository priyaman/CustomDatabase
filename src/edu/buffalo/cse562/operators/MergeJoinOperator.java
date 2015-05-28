package edu.buffalo.cse562.operators;

import java.sql.SQLException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import edu.buffalo.cse562.beans.Schema;
import edu.buffalo.cse562.beans.Tuple;
import edu.buffalo.cse562.eval.Evaluator;
import edu.buffalo.cse562.externalSort.ExternalSort;
import edu.buffalo.cse562.globals.GlobalConstants;
import edu.buffalo.cse562.interfaces.Operator;
import edu.buffalo.cse562.sqlparser.ColDetails;
import edu.buffalo.cse562.utils.ExternalSortUtil;

public class MergeJoinOperator extends BaseOperator implements Operator {

	
	private Operator leftOperator;
	private Operator rightOperator;
	private Schema leftSchema;
	private Schema rightSchema;
	private ColDetails leftCol;
	private ColDetails rightCol;
	private Schema newSchema;
	//HashMap
	private HashMap<LeafValue,List<Tuple>> leftHash;
	//leftReturner
	private Iterator<Tuple> leftReturner = null;
	//Left Tuple
	Tuple leftTuple = null;
	//Right Tuple to return
	Tuple rightTuple = null;
	//Evalator
	Evaluator eval;
	//JoinExpression
	Expression joinExp;

	public MergeJoinOperator(Operator leftOperator, Operator rightOperator, Schema leftSchema, Schema rightSchema,
			ColDetails leftCol, ColDetails rightCol, Expression joinExp) {
		this.leftOperator = leftOperator;
		this.rightOperator = rightOperator;
		this.leftSchema = leftSchema;
		this.rightSchema = rightSchema;
		this.leftCol = leftCol;
		this.rightCol = rightCol;
		this.joinExp = joinExp;
		newSchema = new Schema();
		this.newSchema = new Schema();
		this.newSchema.ColumnMap.putAll(leftSchema.ColumnMap);
		this.newSchema.ColumnMap.putAll(rightSchema.ColumnMap);
		this.newSchema.colIdxMap.putAll(leftSchema.colIdxMap);
		int index1= leftSchema.colIdxMap.size();
		Iterator it = this.leftSchema.colIdxMap.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry<String,Integer> pair = (Map.Entry<String,Integer>)it.next();
	        	int index2 = index1+pair.getValue();
	        		this.newSchema.colIdxMap.put(pair.getKey().toUpperCase(), index2);
	    }
	    
	    //init
	    initialize();
	}
	
	private void initialize(){
		ExternalSort extSort = new ExternalSort(leftOperator, leftSchema);
		String retFilename = extSort.externalSort(leftCol);
		this.leftOperator = new FileReaderOperator(leftSchema,GlobalConstants.SWAP_DIR, retFilename);
		
		extSort = new ExternalSort(rightOperator, rightSchema);
		retFilename = extSort.externalSort(rightCol);
		this.rightOperator = new FileReaderOperator(rightSchema,GlobalConstants.SWAP_DIR, retFilename);
	}
	
	private Tuple mergeTuple(Tuple leftTuple, Tuple rightTuple){
		Tuple t = new Tuple();
		t.tupleItems = new LeafValue[leftTuple.tupleItems.length + rightTuple.tupleItems.length];
		for(int i=0;i<leftTuple.tupleItems.length;i++)
			t.tupleItems[i] = leftTuple.tupleItems[i];
		for(int i=leftTuple.tupleItems.length-1;i<t.tupleItems.length;i++)
			t.tupleItems[i] = rightTuple.tupleItems[i];
		return t;
	}

	@Override
	public Tuple getNext() {
		//Load Tuple from operators if tuple is not null
		if(leftTuple!=null && leftTuple.tupleItems!=null){
			leftTuple = leftOperator.getNext();
		}
		if(rightTuple!=null && rightTuple.tupleItems!=null){
			rightTuple = leftOperator.getNext();
		}
		//Merge
		do{
		Tuple mergedTuple = this.mergeTuple(leftTuple, rightTuple);
		BooleanValue result = null;
		try {
			result = (BooleanValue)eval.eval(joinExp);
		} catch (SQLException e) {
			System.out.println("Archana's code doesnt work. Merge Join Eval Error");
			e.printStackTrace();
		}
		if(result.getValue()){
			leftTuple = leftOperator.getNext();
			rightTuple = rightOperator.getNext();
			return mergedTuple;
		}else{
				MinorThan compareExp = new MinorThan();
				BinaryExpression joinBinaryExp = (BinaryExpression)joinExp;
				compareExp.setLeftExpression(joinBinaryExp.getLeftExpression());
				compareExp.setRightExpression(joinBinaryExp.getRightExpression());
				try {
					result = (BooleanValue)eval.eval(compareExp);
				} catch (SQLException e) {
					System.out.println("Error at merge join compare to load new one.");
					e.printStackTrace();
				}
				if(result.getValue()){
					leftTuple = leftOperator.getNext();
				}else{
					rightTuple = rightOperator.getNext();
				}
			
			}
		}while(leftTuple!=null && rightTuple!=null && leftTuple.tupleItems!=null && rightTuple.tupleItems!=null);
		return null;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		
	}

}
