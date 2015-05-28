package edu.buffalo.cse562;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import edu.buffalo.cse562.beans.Schema;
import edu.buffalo.cse562.beans.Tuple;
import edu.buffalo.cse562.globals.GlobalConstants;
import edu.buffalo.cse562.globals.GlobalConstants.JoinTypes;
import edu.buffalo.cse562.globals.GlobalConstants.RAOperator;
import edu.buffalo.cse562.interfaces.Operator;
import edu.buffalo.cse562.operators.AggregateOperator;
import edu.buffalo.cse562.operators.ExtProjOperator;
import edu.buffalo.cse562.operators.FileReaderOperator;
import edu.buffalo.cse562.operators.GroupByOperator;
import edu.buffalo.cse562.operators.HashJoinOperator;
import edu.buffalo.cse562.operators.MergeJoinOperator;
import edu.buffalo.cse562.operators.OrderByOperator;
import edu.buffalo.cse562.operators.PrintOperator;
import edu.buffalo.cse562.operators.SelectOperator;
import edu.buffalo.cse562.operators.XProductOperator;
import edu.buffalo.cse562.raobjects.AggregateRAObject;
import edu.buffalo.cse562.raobjects.BaseRAObject;
import edu.buffalo.cse562.raobjects.ExProjectRAObject;
import edu.buffalo.cse562.raobjects.GroupByRAObject;
import edu.buffalo.cse562.raobjects.JoinRAObject;
import edu.buffalo.cse562.raobjects.OrderByRAObject;
import edu.buffalo.cse562.raobjects.PrintRAObject;
import edu.buffalo.cse562.raobjects.RelationRAObject;
import edu.buffalo.cse562.raobjects.WhereRAObject;
import edu.buffalo.cse562.sqlparser.ColDetails;
import edu.buffalo.cse562.sqlparser.CreateTableParser;
import edu.buffalo.cse562.sqlparser.SelectParser;
import edu.buffalo.cse562.utils.Utilities;
import edu.buffalo.cse562.utils.statsCollection;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.Union;

public class SqlReader {

	public Map<String, Schema> relations;
	public List<BaseRAObject> relationalAlgebraTreeHeads;
	

	public SqlReader() {
		// TODO Auto-generated constructor stub
	
		relations = new HashMap<String, Schema>();
		relationalAlgebraTreeHeads = new ArrayList<BaseRAObject>();
		
	}

	public void parseSqlFiles(List<File> sqlFiles, String phase) {
		CreateTableParser ctParser = new CreateTableParser();
		statsCollection stCollection = new statsCollection();
		SelectParser selParser = new SelectParser();
		int tempCount=0;
		System.out.println("number of files "+sqlFiles.size());
		
		for (File sql : sqlFiles) {
			try {
				FileReader stream = new FileReader(sql);
				CCJSqlParser parser = new CCJSqlParser(stream);
				Statement stmt = null;
				while ((stmt = parser.Statement()) != null) {
					tempCount++;
					if (stmt instanceof CreateTable && phase.equals(GlobalConstants.QUERY_PHASE)) {
						Schema schema = ctParser
								.parseCreateTable((CreateTable) stmt);
						relations.put(schema.table.getWholeTableName()
								.toUpperCase(), schema);
					} 
					else if (stmt instanceof CreateTable && phase.equals(GlobalConstants.LOAD_PHASE)) {
						 stCollection
								.parseCreateTable((CreateTable) stmt);
						 
						 
						//relations.put(schema.table.getWholeTableName()
							//	.toUpperCase(), schema);
					} 
					
					else if (stmt instanceof Select) {
						SelectBody selBody = ((Select) stmt).getSelectBody();
						if (selBody instanceof Union)
							relationalAlgebraTreeHeads.add(selParser
									.getTreeHeadsFromUnion(selBody));
						else if (selBody instanceof PlainSelect)
							relationalAlgebraTreeHeads
									.add(selParser
											.getTreeHeadFromSelect((PlainSelect) selBody));
						 System.out.println("stmt "+stmt.toString());
							
					}
				}

			} catch (FileNotFoundException e) {
				System.out.println("EXCEPTION: File not found @."
						+ sql.getAbsolutePath());
				e.printStackTrace();
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		// String testFileName = "R";
		// String testColName = "R.A";
		// ExternalSort extSorter = new ExternalSort(relations);
		// extSorter.externalSort(testFileName,
		// relations.get(testFileName).ColumnMap.get(testColName));

		processRelationalAlgebra(relationalAlgebraTreeHeads);
	}

	public void processRelationalAlgebra(
			List<BaseRAObject> relationalAlgebraTreeHeads) {
		for (BaseRAObject treeHead : relationalAlgebraTreeHeads) {
			PrintRAObject printHead = new PrintRAObject();
			printHead.operator = RAOperator.PRINT;
			treeHead.parent = printHead;
			printHead.leftChild = treeHead;
			Limit lim = new Limit();
			if (printHead.leftChild.leftChild instanceof ExProjectRAObject) {
				lim = ((ExProjectRAObject) printHead.leftChild.leftChild).limit;
			} else if (printHead.leftChild.leftChild instanceof GroupByRAObject) {
				lim = ((GroupByRAObject) printHead.leftChild.leftChild).limit;
			} else if (printHead.leftChild.leftChild instanceof AggregateRAObject) {
				lim = ((AggregateRAObject) printHead.leftChild.leftChild).limit;
			}
			else if (printHead.leftChild instanceof ExProjectRAObject) {
				lim = ((ExProjectRAObject) printHead.leftChild).limit;
			} else if (printHead.leftChild instanceof GroupByRAObject) {
				lim = ((GroupByRAObject) printHead.leftChild).limit;
			} else if (printHead.leftChild instanceof AggregateRAObject) {
				lim = ((AggregateRAObject) printHead.leftChild).limit;
			}
			printHead.limit = lim;

			treeHead = printHead;
			getSchemaFromTreeHead(treeHead);
			
			//TEST
			//Utilities.printRATree(treeHead);
			//System.out.println("###################################################");
			for(int i=0;i<3;i++){
				optimize(treeHead);
				}
			optimize2(treeHead);
		//	Utilities.printRATree(treeHead);
			Operator op = createOperatorandReturn(treeHead);
			
		}
	}
	
	public void optimize2(BaseRAObject treeHead) {
		while (treeHead != null) {
			if (treeHead.operator.equals(RAOperator.JOIN)) {
				Expression joinExp = ((JoinRAObject)treeHead).onExp;
				if(joinExp instanceof Parenthesis){
						joinExp =((Parenthesis)joinExp).getExpression();
					}
					if(joinExp instanceof AndExpression){
						List<Expression> exps = splitAndClauses(joinExp);
						((JoinRAObject)treeHead).onExp = exps.remove(0);
						Expression restofselect = null;
						if(exps.size() == 1){
							restofselect = exps.remove(0);
						}else{
							restofselect = new AndExpression();
							for(Expression e:exps){
									((AndExpression)restofselect).setLeftExpression(restofselect);
									((AndExpression)restofselect).setRightExpression(e);
							}
						}
						
						WhereRAObject wherePart = new WhereRAObject(treeHead.parent, treeHead);
						wherePart.exp = restofselect;
						
						wherePart.operator = RAOperator.SELECT;
						
						treeHead.parent.leftChild= wherePart;
						
						wherePart.parent = treeHead.parent;
						treeHead.parent=wherePart;
						
						wherePart.leftChild = treeHead;
						treeHead.parent.inSchema=treeHead.outSchema;
						treeHead.parent.outSchema=treeHead.outSchema;
					}
				}
			
			
			treeHead = treeHead.leftChild;
		}
	}
	public void optimize(BaseRAObject treeHead) {
		while (treeHead != null) {

			if (treeHead.operator.equals(RAOperator.SELECT)) {
				if (treeHead.leftChild.operator.equals(RAOperator.JOIN)) {
					Expression e = ((WhereRAObject) treeHead).exp;
					List<Expression> listExpr = splitAndClauses(e);
					Expression JoinExpression = null;
					Expression leftExpression = null;
					Expression rightExpression = null;
					for (Expression ex : listExpr) {
						boolean leftPresentInLeft = false;
						boolean leftPresentInRight = false;
						boolean rightPresentInLeft = false;
						boolean rightPresentInRight = false;
						/*if(ex instanceof Parenthesis){
							ex =((Parenthesis)ex).getExpression();
						}*/
						Expression[] leftAndRight = checkTypeofExpression(ex);
						if (leftAndRight[0] instanceof Column || ex instanceof Parenthesis) {
							Column col = (Column) leftAndRight[0];
							if(ex instanceof Parenthesis)
								leftPresentInLeft=true;
							
							else
								{leftPresentInLeft = checkIfColumnPresent(
									treeHead.leftChild.leftChild, col);
							leftPresentInRight = checkIfColumnPresent(
									treeHead.leftChild.rightChild, col);}

						}
						if (leftAndRight[1] instanceof Column) {
							Column col = (Column) leftAndRight[1];
							rightPresentInLeft = checkIfColumnPresent(
									treeHead.leftChild.leftChild, col);
							rightPresentInRight = checkIfColumnPresent(
									treeHead.leftChild.rightChild, col);

						}
						if ((leftPresentInLeft && rightPresentInRight)
								|| (leftPresentInRight && rightPresentInLeft)) {// JOIN
							if(leftPresentInRight && rightPresentInLeft)
							{
								 ex = switchExpr(ex);
							}
							if (JoinExpression == null) {
						
								JoinExpression = ex;
							} else {
								JoinExpression = new AndExpression(
										JoinExpression, ex);
							}
						} else if (leftPresentInLeft || rightPresentInLeft) {
							// push down on left
							if (leftExpression == null) {
								leftExpression = ex;
							} else {
								leftExpression = new AndExpression(
										leftExpression, ex);
							}
						} else if (leftPresentInRight || rightPresentInRight) {
							// push down on right
							if (rightExpression == null) {
								rightExpression = ex;
							} else {
								rightExpression = new AndExpression(
										rightExpression, ex);
							}
						}

					}
					if (leftExpression != null) {

						WhereRAObject left = new WhereRAObject(
								treeHead.leftChild);
						left.operator = RAOperator.SELECT;

						left.exp = leftExpression;

						treeHead.leftChild.parent = treeHead.parent;
						treeHead.parent.leftChild = treeHead.leftChild;
						BaseRAObject currHead = treeHead.leftChild;
						left.leftChild = currHead.leftChild;
						currHead.leftChild.parent = left;
						currHead.leftChild = left;
						left.parent = currHead;
						treeHead = currHead;
						currHead.leftChild.inSchema = currHead.leftChild.leftChild.outSchema;
						currHead.leftChild.outSchema = currHead.leftChild.leftChild.outSchema;
					

					}
					if (rightExpression != null) {
						if (treeHead.operator.equals(RAOperator.SELECT)) {
							WhereRAObject right = new WhereRAObject(
									treeHead.leftChild);
							right.operator = RAOperator.SELECT;
							right.exp = rightExpression;
							treeHead.leftChild.parent = treeHead.parent;
							treeHead.parent.leftChild = treeHead.leftChild;
							BaseRAObject currHead = treeHead.leftChild;
							right.leftChild = currHead.rightChild;
							right.inSchema = right.leftChild.outSchema;
							right.outSchema = right.leftChild.outSchema;
							currHead.rightChild.parent = right;
							currHead.rightChild = right;
							right.parent = currHead;
							treeHead = currHead;

						} else {
							WhereRAObject right = new WhereRAObject(
									treeHead.leftChild);
							right.operator = RAOperator.SELECT;
							right.exp = rightExpression;
							treeHead.rightChild.parent = right;
							right.leftChild = treeHead.rightChild;
							right.inSchema = right.leftChild.outSchema;
							right.outSchema = right.leftChild.outSchema;
							treeHead.rightChild = right;

							right.parent = treeHead;

						}

					}
					if (JoinExpression != null) {

						if (treeHead.operator.equals(RAOperator.SELECT)) {
							treeHead.leftChild.parent = treeHead.parent;
							treeHead.parent.leftChild = treeHead.leftChild;
							BaseRAObject currHead = treeHead.leftChild;
							treeHead = currHead;
						}
						
						((JoinRAObject) treeHead).onExp = JoinExpression;
						((JoinRAObject) treeHead).setJoinType();

					}
				}
			}
			if(treeHead.rightChild!=null)
			{
				optimize(treeHead.rightChild);
			}
			treeHead = treeHead.leftChild;
		}

	}

	public boolean checkIfColumnPresent(BaseRAObject joinObject, Column col) {
		boolean isPresent = false;

		Iterator it = joinObject.outSchema.colIdxMap.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, Integer> pair = (Entry<String, Integer>) it
					.next();

			if (col.getWholeColumnName().toUpperCase().equals(pair.getKey())) {
				isPresent = true;
				break;
			}
		}

		return isPresent;
	}
public Expression switchExpr(Expression e)
{Expression[] retExpr = new Expression[2];
if (e instanceof EqualsTo) {
	EqualsTo eq = (EqualsTo) e;
	EqualsTo expr = new EqualsTo();
	expr.setRightExpression(eq.getLeftExpression());
	expr.setLeftExpression(eq.getRightExpression());
	return expr;
			
} else if (e instanceof NotEqualsTo) {
	NotEqualsTo eq = (NotEqualsTo) e;
	NotEqualsTo expr = new NotEqualsTo();
	expr.setRightExpression(eq.getLeftExpression());
	expr.setLeftExpression(eq.getRightExpression());
	return expr;
	
} else if (e instanceof GreaterThan) {
	GreaterThan eq = (GreaterThan) e;
	GreaterThan expr = new GreaterThan();
	expr.setRightExpression(eq.getLeftExpression());
	expr.setLeftExpression(eq.getRightExpression());
	return expr;
} else if (e instanceof GreaterThanEquals) {
	GreaterThanEquals eq = (GreaterThanEquals) e;
	GreaterThanEquals expr = new GreaterThanEquals();
	expr.setRightExpression(eq.getLeftExpression());
	expr.setLeftExpression(eq.getRightExpression());
	return expr;
} else if (e instanceof MinorThan) {
	MinorThan eq = (MinorThan) e;
	MinorThan expr = new MinorThan();
	expr.setRightExpression(eq.getLeftExpression());
	expr.setLeftExpression(eq.getRightExpression());
	return expr;
} else if (e instanceof MinorThanEquals) {
	MinorThanEquals eq = (MinorThanEquals) e;
	MinorThanEquals expr = new MinorThanEquals();
	expr.setRightExpression(eq.getLeftExpression());
	expr.setLeftExpression(eq.getRightExpression());
	return expr;
}

	return null;
	}
	public Expression[] checkTypeofExpression(Expression e) {
		Expression[] retExpr = new Expression[2];
	/*	if(e instanceof Parenthesis){
			Parenthesis e2 = (Parenthesis)e;
			e =e2.getExpression();
		}*/
		if (e instanceof EqualsTo) {
			EqualsTo eq = (EqualsTo) e;
			retExpr[0] = eq.getLeftExpression();
			retExpr[1] = eq.getRightExpression();
		} else if (e instanceof NotEqualsTo) {
			NotEqualsTo eq = (NotEqualsTo) e;
			retExpr[0] = eq.getLeftExpression();
			retExpr[1] = eq.getRightExpression();
		} else if (e instanceof GreaterThan) {
			GreaterThan eq = (GreaterThan) e;
			retExpr[0] = eq.getLeftExpression();
			retExpr[1] = eq.getRightExpression();
		} else if (e instanceof GreaterThanEquals) {
			GreaterThanEquals eq = (GreaterThanEquals) e;
			retExpr[0] = eq.getLeftExpression();
			retExpr[1] = eq.getRightExpression();
		} else if (e instanceof MinorThan) {
			MinorThan eq = (MinorThan) e;
			retExpr[0] = eq.getLeftExpression();
			retExpr[1] = eq.getRightExpression();
		} else if (e instanceof MinorThanEquals) {
			MinorThanEquals eq = (MinorThanEquals) e;
			retExpr[0] = eq.getLeftExpression();
			retExpr[1] = eq.getRightExpression();
		}
		return retExpr;

	}

	public List<Expression> splitAndClauses(Expression e) {
		List<Expression> retList = new ArrayList<Expression>();
		if (e instanceof AndExpression) {
			AndExpression a = (AndExpression) e;
			retList.addAll(splitAndClauses(a.getLeftExpression()));
			retList.addAll(splitAndClauses(a.getRightExpression()));
		} else {
			retList.add(e);
		}
		return retList;
	}

	public void getSchemaFromTreeHead(BaseRAObject treeHead) {
		while (treeHead.leftChild != null) {
			treeHead = treeHead.leftChild;
		}
		do {
			if (treeHead.operator.equals(RAOperator.PRINT)) {
				// PrintRAObject printRA = (PrintRAObject)treeHead;
			}
			if(treeHead.operator.equals(RAOperator.ORDER_BY))
			{
				OrderByRAObject orderObj = (OrderByRAObject) treeHead;
				((OrderByRAObject) treeHead)
				.createSchema(treeHead.leftChild.outSchema);
				
			}

			if (treeHead.operator.equals(RAOperator.RELATION)) {
				RelationRAObject relationRA = (RelationRAObject) treeHead;
				Schema currSchema;
				if (relationRA.alias != null) {
					currSchema = new Schema();
					Schema tempSchema = new Schema();
					tempSchema = relations.get(relationRA.tablename
							.toUpperCase());
					currSchema.table.setName(tempSchema.table.getName());
					currSchema.table.setAlias(relationRA.alias);
					Iterator it = tempSchema.ColumnMap.entrySet().iterator();
					while (it.hasNext()) {
						Map.Entry<String, ColDetails> pair = (Entry<String, ColDetails>) it
								.next();

						String key = pair.getKey();
						ColDetails cd = pair.getValue();
						Table tab= new Table();
						tab.setName(relationRA.alias.toUpperCase());
						cd.colDetails.setTable(tab);
						
						String key1 = relationRA.alias.toUpperCase() + GlobalConstants.DOT + key.split("\\.")[1].toUpperCase();
						cd.colDef.setColumnName(key1);
						currSchema.ColumnMap.put(key1,
								cd);

					}
					Iterator idx = tempSchema.colIdxMap.entrySet().iterator();
					while (idx.hasNext()) {
						Map.Entry<String, Integer> pair = (Entry<String, Integer>) idx
								.next();
						String key = pair.getKey();
						currSchema.colIdxMap.put(relationRA.alias.toUpperCase()
								+ GlobalConstants.DOT + key.split("\\.")[1].toUpperCase(),
								pair.getValue());

					}
				} else
					currSchema = relations.get(relationRA.tablename
							.toUpperCase());
				((RelationRAObject) treeHead).outSchema = currSchema;
			} else if (treeHead.operator.equals(RAOperator.SELECT)) {
				((WhereRAObject) treeHead)
						.createSchema(treeHead.leftChild.outSchema);
			} else if (treeHead.operator.equals(RAOperator.JOIN)) {
				treeHead.rightChild.parent = null;
				getSchemaFromTreeHead(treeHead.rightChild);
				treeHead.rightChild.parent = treeHead;
				((JoinRAObject) treeHead).createSchema(
						treeHead.leftChild.outSchema,
						treeHead.rightChild.outSchema);
				// Do Join
				// #1: Nested Loop
				// #2:Grace Hybrid
				// #3: Sort Merge
			} else if (treeHead.operator.equals(RAOperator.GROUP_BY)) {
				GroupByRAObject groupRA = (GroupByRAObject) treeHead;
				((GroupByRAObject) treeHead)
						.createSchema(treeHead.leftChild.outSchema);
			} else if (treeHead.operator.equals(RAOperator.AGGREGATE)) {

				((AggregateRAObject) treeHead)
						.createSchema(treeHead.leftChild.outSchema);

			} else if (treeHead.operator.equals(RAOperator.EXTENDED_PROJECT)) {

				((ExProjectRAObject) treeHead)
						.createSchema(treeHead.leftChild.outSchema);

			} else if (treeHead.operator.equals(RAOperator.UNION)) {

			} else if (treeHead.operator.equals(RAOperator.ORDER_BY)) {

			}
			// treeHead.operation = prevOpr;
			treeHead = treeHead.parent;
		} while (treeHead != null && treeHead.operator != null);

	}

	Operator createOperatorandReturn(BaseRAObject treeHead) {
		
		Operator prevOpr = null;
		while (treeHead.leftChild != null) {
			treeHead = treeHead.leftChild;
		}
		do {
			if (treeHead.operator.equals(RAOperator.PRINT)) {
				// For relation, we get the schema from the create table
				// statements
				PrintRAObject printRA = (PrintRAObject) treeHead;

				PrintOperator printOpr = new PrintOperator(prevOpr,
						printRA.limit);
				Tuple tuple = printOpr.getNext();

				prevOpr = printOpr;
			}
			if(treeHead.operator.equals(RAOperator.ORDER_BY))
			{
				OrderByRAObject orderRA = (OrderByRAObject) treeHead;
				prevOpr = new OrderByOperator(prevOpr,orderRA.orderByItems,orderRA.outSchema);
			}
			if (treeHead.operator.equals(RAOperator.RELATION)) {
				RelationRAObject relationRA = (RelationRAObject) treeHead;
				prevOpr = new FileReaderOperator(relationRA.outSchema);
			} else if (treeHead.operator.equals(RAOperator.SELECT)) {
				WhereRAObject whereRA = (WhereRAObject) treeHead;
				SelectOperator selOpr = new SelectOperator(prevOpr,
						whereRA.exp, treeHead.inSchema);
				prevOpr = selOpr;
			} else if (treeHead.operator.equals(RAOperator.JOIN)) {
			
				Operator leftOpr = prevOpr;
				treeHead.rightChild.parent = null;
				Operator rightOpr = this
						.createOperatorandReturn(treeHead.rightChild);
				treeHead.rightChild.parent = treeHead;
				JoinRAObject joinOpr = (JoinRAObject) treeHead;

				
				ColDetails leftCol = null;
				ColDetails rightCol = null;
				if(!joinOpr.joinType.equals(JoinTypes.CROSS)){
					BinaryExpression binExp = (BinaryExpression) joinOpr.onExp;
					leftCol = joinOpr.leftSchema.ColumnMap.get(binExp
							.getLeftExpression().toString().toUpperCase());
					rightCol = joinOpr.rightSchema.ColumnMap.get(binExp
							.getRightExpression().toString().toUpperCase());
				//	System.out.println("leftSchema:" + joinOpr.leftSchema.ColumnMap);
				//	System.out.println("rightSchema:" + joinOpr.rightSchema.ColumnMap);
				//	System.out.println("BinaryExp:" + binExp);
				}
				Operator xOpr = null;
				switch(joinOpr.joinType){
					case HASH:
						 	xOpr = new HashJoinOperator(leftOpr,
								rightOpr, joinOpr.leftSchema, joinOpr.rightSchema,
								joinOpr.outSchema, leftCol, rightCol, joinOpr.onExp);
						 	((HashJoinOperator)xOpr).schema = treeHead.outSchema;
						break;
					case MERGE:
							xOpr = new MergeJoinOperator(leftOpr,
								rightOpr, joinOpr.leftSchema, joinOpr.rightSchema,
								leftCol, rightCol, joinOpr.onExp);
							((MergeJoinOperator)xOpr).schema = treeHead.outSchema;
							break;
					case CROSS:
							xOpr = new XProductOperator(leftOpr,
								rightOpr,joinOpr.outSchema);
							((XProductOperator)xOpr).schema = treeHead.outSchema;
				}
				
				prevOpr = xOpr;

			} else if (treeHead.operator.equals(RAOperator.GROUP_BY)) {
				GroupByRAObject groupRA = (GroupByRAObject) treeHead;
				GroupByOperator gpOpr = new GroupByOperator(prevOpr,
						treeHead.inSchema, groupRA.columnReferences,
						groupRA.items, groupRA.alias);
				prevOpr = gpOpr;
			} else if (treeHead.operator.equals(RAOperator.AGGREGATE)) {
				AggregateRAObject aggrObj = (AggregateRAObject) treeHead;
				AggregateOperator aggrOpr = new AggregateOperator(prevOpr,
						treeHead.inSchema, aggrObj.items, aggrObj.alias);

				prevOpr = aggrOpr;

			} else if (treeHead.operator.equals(RAOperator.EXTENDED_PROJECT)) {
				ExProjectRAObject projObj = (ExProjectRAObject) treeHead;
				ExtProjOperator projOpr = new ExtProjOperator(prevOpr,
						treeHead.inSchema, projObj.items, projObj.alias);
				prevOpr = projOpr;
			} else if (treeHead.operator.equals(RAOperator.UNION)) {

			} else if (treeHead.operator.equals(RAOperator.ORDER_BY)) {

			}
			treeHead = treeHead.parent;
		} while (treeHead != null && treeHead.operator != null);

		return prevOpr;
	}
}
