package edu.buffalo.cse562.operators;

import net.sf.jsqlparser.expression.LeafValue;
import edu.buffalo.cse562.beans.Schema;
import edu.buffalo.cse562.beans.Tuple;
import edu.buffalo.cse562.interfaces.Operator;

public class XProductOperator extends BaseOperator implements Operator{

	Operator rel1;
	Operator rel2;
	Tuple cachedRel1Tuple;
	boolean itr1or2 = true;
		public XProductOperator(Operator rel1, Operator rel2, Schema schema) {
			this.rel1=rel1;
			this.rel2=rel2;
			this.schema=schema;
	}
	@Override
	public Tuple getNext() {
		if(itr1or2){
			cachedRel1Tuple = rel1.getNext();
			if(cachedRel1Tuple==null || cachedRel1Tuple.tupleItems==null)
				return null;
			rel2.reset();
			itr1or2 = !itr1or2;
			return this.getNext();
		}else{
			//Read from rel2
			Tuple r2Tuple = rel2.getNext();
			if(r2Tuple==null || r2Tuple.tupleItems==null){
				itr1or2 = !itr1or2;
				return this.getNext();
			}
			//Concat both rows
			Tuple retTuple = new Tuple();
			retTuple.tupleItems = new LeafValue[this.schema.ColumnMap.size()];
			int rctr=0;
			int ctr = 0;
			do{
				retTuple.tupleItems[rctr++] = cachedRel1Tuple.tupleItems[ctr++];
			}while(ctr<cachedRel1Tuple.tupleItems.length);
			//Reset ctr
			ctr = 0;
			do{
				retTuple.tupleItems[rctr++] = r2Tuple.tupleItems[ctr++];
			}while(ctr<r2Tuple.tupleItems.length);
			return retTuple;
		}
	}
	@Override
	public void reset() {
		this.rel1.reset();
		this.rel2.reset();
	}

}
