package edu.buffalo.cse562.operators;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
import edu.buffalo.cse562.globals.GlobalConstants;
import edu.buffalo.cse562.interfaces.Operator;
import edu.buffalo.cse562.sqlparser.ColDetails;

public class GroupByOperator extends BaseOperator implements Operator {
	class Leaf
	{
		LeafValue[] leafVal;
		int count;
	}
	Operator input;
	boolean getNextCalled=false;
	List<Column> columnReferences;
	List<SelectItem> items;
	public HashMap<String,Leaf> hm1;
	List<Tuple> gpByList;
	String Alias;
	public  GroupByOperator(Operator input, Schema schema, List<Column> columnReferences, List<SelectItem> items,String alias ) {

		getNextCalled=false;
		this.input=input;
		this.schema=schema;
		this.columnReferences=columnReferences;
		this.items=items;
		this.Alias=alias;
		hm1 = new  HashMap<String,Leaf>();
	}

	List AvgLList = new ArrayList();

	@Override

	public Tuple getNext() {
		
		Tuple tuple=null;	
		if(!getNextCalled)
		{
			gpByList = innergetNext();
			getNextCalled=true;
			
		}
		
		if(gpByList.size()>0)
		{
			tuple = gpByList.get(0);
			gpByList.remove(0);
		}
		return tuple;
		// TODO Auto-generated method stub
	}
	public List<Tuple> innergetNext(){
		
		//LeafValue [] tuple=null;
		Tuple tuple;
		tuple=input.getNext();
		while(tuple!=null && tuple.tupleItems!=null)
		{
			//for each row:
			Evaluator eval = new Evaluator(this.schema, tuple.tupleItems);

			String key="";
			//Evaluate column references first:
			for(int j=0;j<columnReferences.size();j++)
			{
				LeafValue result=null;

				try {
					result = eval.eval(columnReferences.get(j));
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				key=key+"."+result.toString();

			}
			Leaf v = new Leaf();

			boolean notFound=false;

			if(hm1.get(key)==null)
			{
				notFound=true;

				v.leafVal = new LeafValue[this.items.size()];
				v.count=1;
			}
			else
			{
				v.leafVal = hm1.get(key).leafVal;
				v.count=hm1.get(key).count;
				v.count=v.count+1;
				//System.out.println("key "+key+" count "+v.count);
			}
			int selectCount=0;
			Tuple retTuple = new Tuple();
			retTuple.tupleItems=new LeafValue[items.size()+1];
			for(int i=0;i<this.items.size();i++)
			{


				if(((SelectExpressionItem)items.get(i)).getExpression() instanceof Function )
				{

					SelectExpressionItem selExp = (SelectExpressionItem) items.get(i);
					Function f = (Function) selExp.getExpression();
					String fname = f.getName().toUpperCase();
					if(fname.equals(GlobalConstants.COUNT))
					{
						if(notFound)
						{

							v.leafVal[selectCount]=new LongValue("1");
							//aggrRes[selectCount]=this.hm.get(f.toString()+selectCount);
						}
						else
						{
							LongValue lv=(LongValue) v.leafVal[selectCount];
							Long l =lv.getValue()+1;
							LongValue lv1 = new LongValue(l.toString());
							v.leafVal[selectCount]=lv1;

							//aggrRes[selectCount]=this.hm.get(f.toString()+selectCount);
						}
					}
					if(f.getName().equalsIgnoreCase("sum"))
					{
						LeafValue l1=null;

						if(notFound)
						{

							try {
								l1= eval.eval((Expression)f.getParameters().getExpressions().get(0));
									


								
							} catch (SQLException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} 
							v.leafVal[selectCount]=l1;
							

						}
						else
						{
							try {
								l1= eval.eval((Expression)f.getParameters().getExpressions().get(0));
							} catch (SQLException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							
								Double lv = null;
								try {
									lv = v.leafVal[selectCount].toDouble() + l1.toDouble();
								} catch (InvalidLeaf e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
								v.leafVal[selectCount] = new DoubleValue(lv.toString());
							
							//aggrRes[selectCount]=this.hm.get(f.toString()+selectCount);


						}

					}
					if(fname.equals(GlobalConstants.AVG))
					{
						LeafValue l1=null;

						if(notFound)
						{

							try {
								l1= eval.eval((Expression)f.getParameters().getExpressions().get(0));
									l1=(DoubleValue)l1;

							} catch (SQLException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} 
							v.leafVal[selectCount]=l1;
							

						}
						else
						{


							try {
								l1= eval.eval((Expression)f.getParameters().getExpressions().get(0));
							} catch (SQLException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							
								Double lv = null;
								try {
									lv = v.leafVal[selectCount].toDouble() + l1.toDouble();
								} catch (InvalidLeaf e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
								v.leafVal[selectCount]= new DoubleValue(lv.toString());
							


						}

					}

					if(fname.equals(GlobalConstants.MAX))
					{
						LeafValue l1=null;

						if(notFound)
						{

							try {

								l1= eval.eval((Expression)f.getParameters().getExpressions().get(0));
								l1=(DoubleValue)l1;

							} catch (SQLException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} 
							v.leafVal[selectCount]=l1;
							//aggrRes[selectCount]=this.hm.get(f.toString()+selectCount);
						}
						else
						{

							try {
								l1= eval.eval((Expression)f.getParameters().getExpressions().get(0));
							} catch (SQLException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							LeafValue lv;

								Double max = null;
								try {
									max = Math.max(v.leafVal[selectCount].toDouble(), l1.toDouble());
								} catch (InvalidLeaf e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								} 
								lv=new DoubleValue(max.toString());
							v.leafVal[selectCount]=lv;
							//aggrRes[selectCount]=this.hm.get(f.toString()+selectCount);

						}

					}
					if(fname.equals(GlobalConstants.MIN))
					{
						LeafValue l1=null;

						if(!notFound)
						{

							try {
								l1= eval.eval((Expression)f.getParameters().getExpressions().get(0));
									l1=(DoubleValue)l1;
							} catch (SQLException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} 

							v.leafVal[selectCount]=l1;

							//aggrRes[selectCount]=this.hm.get(f.toString()+selectCount);
						}
						else
						{

							try {
								l1= eval.eval((Expression)f.getParameters().getExpressions().get(0));
							} catch (SQLException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							LeafValue lv;
							Double min = null;
							try {
								min = Math.min(v.leafVal[selectCount].toDouble(), l1.toDouble());
							} catch (InvalidLeaf e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} 
							lv=new DoubleValue(min.toString());
							v.leafVal[selectCount]=lv;

							//aggrRes[selectCount]=this.hm.get(f.toString()+selectCount);

						}

					}

				}
				else
				{
					Expression exp = ((SelectExpressionItem)items.get(i)).getExpression();
					try {
						v.leafVal[i]=eval.eval(exp);
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				}


				selectCount++;
			}
			hm1.put(key, v);
			tuple=input.getNext();
		}
		List<Integer> avgList = new ArrayList<Integer>();
		for(int i=0;i<this.items.size();i++)
		{
			if(((SelectExpressionItem)items.get(i)).getExpression() instanceof Function )
			{

				SelectExpressionItem selExp = (SelectExpressionItem) items.get(i);
				Function f = (Function) selExp.getExpression();	
				if((f.getName().toUpperCase().equals(GlobalConstants.AVG)))
					avgList.add(i);
			}

		}
		List<Tuple> retList = new ArrayList<Tuple>();
		//List<LeafValue[]> retList = new ArrayList<LeafValue[]>();
		Iterator it = hm1.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String,Leaf> pair = (Entry<String, Leaf>)it.next();
			Tuple T = new Tuple();
			Leaf l = pair.getValue();
			for(int i=0;i<l.leafVal.length;i++)
			{
				for(int j=0;j<avgList.size();j++)
				{
					if(avgList.get(j)==i)
					{	
						
							try {
								((DoubleValue)l.leafVal[i]).setValue(l.leafVal[i].toDouble()/l.count);
							} catch (InvalidLeaf e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
								
						
					}
				}
			}
			T.tupleItems=l.leafVal;
			retList.add(T);
			


		}

		return retList;
	}


	@Override
	public void reset() {
		// TODO Auto-generated method stub

	}

}