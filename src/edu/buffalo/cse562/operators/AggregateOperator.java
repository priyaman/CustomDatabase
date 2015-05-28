package edu.buffalo.cse562.operators;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.LeafValue.InvalidLeaf;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import edu.buffalo.cse562.beans.Schema;
import edu.buffalo.cse562.beans.Tuple;
import edu.buffalo.cse562.eval.Evaluator;
import edu.buffalo.cse562.interfaces.Operator;
import edu.buffalo.cse562.sqlparser.ColDetails;

public class AggregateOperator extends BaseOperator implements Operator {

	Operator input;
	List<SelectItem> items;
	public HashMap<String,LeafValue> hm;
	String Alias;
	public AggregateOperator(Operator input, Schema schema, List<SelectItem> selectItems, String alias) {
		this.input=input;
		this.schema=schema;
		this.items=selectItems;
		this.Alias=alias;
		this.hm = new HashMap<String,LeafValue>();
			}

	@Override
	public Tuple getNext() {
		
		Tuple retTuple = new Tuple();
		Tuple tuple;
		retTuple.tupleItems=new LeafValue[items.size()];
		tuple=this.input.getNext();
		if(tuple==null || tuple.tupleItems==null)
			return null;
		int avgCount=0;
		int selectCount=0;
		while(tuple!=null && tuple.tupleItems!=null)
		{
			selectCount=0;
			for(SelectItem sel : this.items)
			{
				SelectExpressionItem selExp = (SelectExpressionItem) sel;
				Function f = (Function) selExp.getExpression();	
				if(f.getName().equalsIgnoreCase("count"))
				{
					if(hm.get(f.toString()+selectCount)==null)
					{
						this.hm.put(f.toString()+selectCount, new LongValue("1"));
						retTuple.tupleItems[selectCount]=this.hm.get(f.toString()+selectCount);
					}
					else
					{
						LongValue lv=(LongValue) this.hm.get(f.toString()+selectCount);
						Long l =lv.getValue()+1;
						LongValue lv1 = new LongValue(l.toString());
						this.hm.put(f.toString()+selectCount, lv1);
						retTuple.tupleItems[selectCount]=this.hm.get(f.toString()+selectCount);
					}
				}
				if(f.getName().equalsIgnoreCase("sum") || f.getName().equalsIgnoreCase("avg"))
				{
					LeafValue l1=null;
					if(f.getName().equalsIgnoreCase("avg"))
						avgCount++;

					if(!hm.containsKey(f.toString()+selectCount))
					{
						Evaluator eval = new Evaluator(this.schema, tuple.tupleItems);
						try {
							l1= eval.eval((Expression)f.getParameters().getExpressions().get(0));
							if(l1 instanceof DoubleValue)
							{
								l1=new DoubleValue(l1.toDouble()+"");


							}
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (InvalidLeaf e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						this.hm.put(f.toString()+selectCount, l1);
						retTuple.tupleItems[selectCount]=this.hm.get(f.toString()+selectCount);

					}
					else
					{

						Evaluator eval = new Evaluator(this.schema, tuple.tupleItems);
						try {
							l1= eval.eval((Expression)f.getParameters().getExpressions().get(0));
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						LeafValue lv1;
						if(l1 instanceof DoubleValue)
						{
							double lv = ((DoubleValue) this.hm.get(f.toString()+selectCount)).getValue() + ( (DoubleValue) l1).getValue();
							lv1 = new DoubleValue(lv+"");

						}
						else
						{
							long lv = ((LongValue) this.hm.get(f.toString()+selectCount)).getValue() + ( (LongValue) l1).getValue();
							lv1 = new LongValue(lv+"");

						}
						this.hm.put(f.toString()+selectCount, lv1);
						retTuple.tupleItems[selectCount]=this.hm.get(f.toString()+selectCount);


					}

				}
				if(f.getName().equalsIgnoreCase("max"))
				{
					LeafValue l1=null;

					if(!hm.containsKey(f.toString()+selectCount))
					{
						Evaluator eval = new Evaluator(this.schema, tuple.tupleItems);
						try {

							l1= eval.eval((Expression)f.getParameters().getExpressions().get(0));
							if(l1 instanceof DoubleValue)
							{
								l1=new DoubleValue(l1.toDouble()+"");


							}

						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (InvalidLeaf e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						this.hm.put(f.toString()+selectCount, l1);
						retTuple.tupleItems[selectCount]=this.hm.get(f.toString()+selectCount);
					}
					else
					{
						Evaluator eval = new Evaluator(this.schema, tuple.tupleItems);
						try {
							l1= eval.eval((Expression)f.getParameters().getExpressions().get(0));
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						LeafValue lv;
						if(l1 instanceof DoubleValue)
						{

							double max =Math.max(((DoubleValue)(this.hm.get(f.toString()+selectCount))).getValue(), ((DoubleValue) l1).getValue()); 
							lv=new DoubleValue(max+"");
						}
						else
						{
							Long max =Math.max(((LongValue)(this.hm.get(f.toString()+selectCount))).getValue(), ((LongValue) l1).getValue()); 
							lv=new LongValue(max+"");

						}
						this.hm.put(f.toString()+selectCount, lv);
						retTuple.tupleItems[selectCount]=this.hm.get(f.toString()+selectCount);

					}

				}
				if(f.getName().equalsIgnoreCase("min"))
				{
					LeafValue l1=null;

					if(!hm.containsKey(f.toString()+selectCount))
					{
						Evaluator eval = new Evaluator(this.schema, tuple.tupleItems);
						try {
							l1= eval.eval((Expression)f.getParameters().getExpressions().get(0));
							if(l1 instanceof DoubleValue)
							{
								l1=new DoubleValue(l1.toDouble()+"");


							}
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (InvalidLeaf e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}

						this.hm.put(f.toString()+selectCount, l1);

						retTuple.tupleItems[selectCount]=this.hm.get(f.toString()+selectCount);
					}
					else
					{
						Evaluator eval = new Evaluator(this.schema, tuple.tupleItems);
						try {
							l1= eval.eval((Expression)f.getParameters().getExpressions().get(0));
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						LeafValue lv;
						if(l1 instanceof DoubleValue)
						{

							double min =Math.min(((DoubleValue)(this.hm.get(f.toString()+selectCount))).getValue(), ((DoubleValue) l1).getValue()); 
							lv=new DoubleValue(min+"");

						}
						else
						{
							long min =Math.min(((LongValue)(this.hm.get(f.toString()+selectCount))).getValue(), ((LongValue) l1).getValue()); 
							lv=new LongValue(min+"");

						}
						this.hm.put(f.toString()+selectCount, lv);

						retTuple.tupleItems[selectCount]=this.hm.get(f.toString()+selectCount);

					}

				}
				selectCount++;
			}
			tuple=this.input.getNext();
			
		}

	


	


int count=0;
for(int i=0;i<items.size();i++)
{
	Function f = (Function) ((SelectExpressionItem)items.get(i)).getExpression();
	if(f.getName().equalsIgnoreCase("avg"))
		count++;

}
if(count!=0)
	avgCount=avgCount/count;
for(int i=0;i<items.size();i++)
{
	Function f = (Function) ((SelectExpressionItem) items.get(i)).getExpression();
	if(f.getName().equalsIgnoreCase("avg"))
	{
		LeafValue l= retTuple.tupleItems[i];
		if(l instanceof DoubleValue)
		{

			double dv = (((DoubleValue) l).getValue())/(double)avgCount;
			DoubleValue dvv = new DoubleValue(dv+"");
			retTuple.tupleItems[i]=dvv;
		}
		else
		{
			long dv = (((LongValue) l).getValue())/(long)avgCount;
			LongValue dvv = new LongValue(dv+"");

			retTuple.tupleItems[i]=dvv;
		}
	}
}

return retTuple;

}

@Override
public void reset() {
	this.input.reset();
}

}
