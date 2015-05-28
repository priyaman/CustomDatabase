package edu.buffalo.cse562.sqlparser;

import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Union;
import edu.buffalo.cse562.globals.GlobalConstants;
import edu.buffalo.cse562.globals.GlobalConstants.RAOperator;
import edu.buffalo.cse562.raobjects.AggregateRAObject;
import edu.buffalo.cse562.raobjects.BaseRAObject;
import edu.buffalo.cse562.raobjects.ExProjectRAObject;
import edu.buffalo.cse562.raobjects.GroupByRAObject;
import edu.buffalo.cse562.raobjects.JoinRAObject;
import edu.buffalo.cse562.raobjects.OrderByRAObject;
import edu.buffalo.cse562.raobjects.RelationRAObject;
import edu.buffalo.cse562.raobjects.UnionRAObject;
import edu.buffalo.cse562.raobjects.WhereRAObject;

public class SelectParser {

	public SelectParser() {
		// TODO Auto-generated constructor stub
	}

	public BaseRAObject getTreeHeadsFromUnion(SelectBody selBody){
		UnionRAObject unionRAObject = new UnionRAObject();
		unionRAObject.operator = RAOperator.UNION;
		List<PlainSelect> plainSelects = ((Union)selBody).getPlainSelects();
		for(PlainSelect stmt:plainSelects){
			unionRAObject.statementHeads.add(getTreeHeadFromSelect(stmt));
		}
		return unionRAObject;
	}

	public BaseRAObject getTreeHeadFromSelect(PlainSelect stmt){
		BaseRAObject treeHead = null ;//= new BaseRAObject();
		BaseRAObject currHead = new BaseRAObject();
		if(null!=stmt){
			if(stmt.getOrderByElements()!=null)
			{
				OrderByRAObject orderObj = new OrderByRAObject();
				orderObj.orderByItems = stmt.getOrderByElements();
				
				
				orderObj.operator=RAOperator.ORDER_BY;
				orderObj.parent=currHead;
				if(currHead.leftChild==null)
					currHead.leftChild = orderObj;
				else
					currHead.rightChild = orderObj;
				currHead = orderObj;
				treeHead=currHead;
				
			}
			//Projection
			if(null!=stmt.getSelectItems()){
				//Projection Items
				List<SelectItem> selectItems = stmt.getSelectItems();
				boolean aggr=false;
				boolean normal=false;
				
				
//				if(stmt.getOrderByElements()!=null)
//				{
//					OrderByRAObject orderBy = new OrderByRAObject();
//					orderBy.operator = RAOperator.ORDER_BY;
//					orderBy.
//				}
				for(SelectItem s : selectItems){
					if(s instanceof SelectExpressionItem){
						if(((SelectExpressionItem) s).getExpression() instanceof Function)
						{
							aggr=true;
						//	GlobalConstants.PROJECT_SELECT_LIST.add(((Function)((SelectExpressionItem) s).getExpression()).getParameters().getExpressions().get(0).toString());
						}
						else
						{
							normal=true;
						}
					}
				}
				if((aggr && normal) || (null!=stmt.getGroupByColumnReferences()) )
				{
					Limit limit = stmt.getLimit();
					GroupByRAObject gpBy = new GroupByRAObject(stmt.getGroupByColumnReferences());
					gpBy.operator = RAOperator.GROUP_BY;
					gpBy.items=stmt.getSelectItems();
					gpBy.limit=limit;
					gpBy.parent = currHead;
					if(currHead.leftChild==null)
						currHead.leftChild = gpBy;
					else
						currHead.rightChild = gpBy;
					currHead = gpBy;
					if(treeHead==null)
					treeHead=currHead;

				} 
				else if(aggr && !normal)
				{
					Limit limit = stmt.getLimit();
					AggregateRAObject aggrObj =new AggregateRAObject();
					aggrObj.operator = RAOperator.AGGREGATE;
					aggrObj.limit=limit;
					aggrObj.parent = currHead;
					aggrObj.items = stmt.getSelectItems();
					if(currHead.leftChild==null)
						currHead.leftChild = aggrObj;
					else
						currHead.rightChild = aggrObj;
					currHead = aggrObj;
					if(treeHead==null)
					treeHead=currHead;
				}
				else if(!aggr && normal)
				{
					Limit limit = stmt.getLimit();
					ExProjectRAObject extProj = new ExProjectRAObject();
					extProj.limit=limit;
					extProj	.parent = currHead;
					extProj.operator = RAOperator.EXTENDED_PROJECT;
					extProj.items = stmt.getSelectItems();
					
					currHead.leftChild = extProj;
					currHead = extProj;
					if(treeHead==null)
					treeHead=currHead;
				}
			}
			//Grouping
			/*if(null!=stmt.getGroupByColumnReferences()){
				GroupByRAObject gpBy = new GroupByRAObject(stmt.getGroupByColumnReferences());
				gpBy.operator = RAOperator.GROUP_BY;
				gpBy.parent = currHead;
				if(currHead.leftChild==null)
					currHead.leftChild = gpBy;
				else
					currHead.rightChild = gpBy;
				currHead = gpBy;
			}*/
			//Selection
			if(null!=stmt.getWhere()){
				WhereRAObject selectOb = new WhereRAObject(currHead);
				selectOb.operator = RAOperator.SELECT;
				selectOb.parent = currHead;
				selectOb.exp = stmt.getWhere();
				if(currHead.leftChild==null)
					currHead.leftChild = selectOb;
				else
					currHead.rightChild = selectOb;
				currHead = selectOb;
			}
			//Joins: Create a Left Deep Tree
			if(null!=stmt.getFromItem()){
				//To be returned
				BaseRAObject relationHead = null;
				//Deepest Node
				BaseRAObject relation1 = null;
				if(stmt.getFromItem() instanceof Table){
					relation1 = new RelationRAObject();
					relation1.operator = RAOperator.RELATION;
					((RelationRAObject)relation1).tablename = ((Table)stmt.getFromItem()).getName();
					((RelationRAObject)relation1).alias=stmt.getFromItem().getAlias();
				}
				else if(stmt.getFromItem() instanceof SubSelect){
					PlainSelect s = ((PlainSelect)((SubSelect)stmt.getFromItem()).getSelectBody());
					relation1=getTreeHeadFromSubSelect(s,stmt.getFromItem().getAlias());
				}
				if(null!=stmt.getJoins()){
					List<Join> joins = stmt.getJoins();
					Iterator<Join> joinItr = joins.iterator();
					Join join = joinItr.next();
					JoinRAObject joinRAObject = new JoinRAObject();
					relation1.parent = joinRAObject;
					joinRAObject.leftChild = (BaseRAObject)relation1;

					BaseRAObject relation2 = null;
					if(join.getRightItem() instanceof Table){
						relation2= new RelationRAObject();
						relation2.operator = RAOperator.RELATION;
						relation2.parent = joinRAObject;
						((RelationRAObject)relation2).tablename = ((Table)join.getRightItem()).getName();
						((RelationRAObject)relation2).alias=join.getRightItem().getAlias();
					}else if(join.getRightItem() instanceof SubSelect){
						if(((SubSelect) join.getRightItem()).getSelectBody() instanceof PlainSelect)

							relation2 = getTreeHeadFromSubSelect((PlainSelect)((SubSelect) join.getRightItem()).getSelectBody(),join.getRightItem().getAlias());
						else if(((SubSelect) join.getRightItem()).getSelectBody() instanceof Union)
							relation2 = getTreeHeadFromUnion((Union)((SubSelect) join.getRightItem()).getSelectBody(),join.getRightItem().getAlias());
					}
					joinRAObject.rightChild = (BaseRAObject)relation2;
					if(null!=join.getOnExpression())
						joinRAObject.onExp = join.getOnExpression();
					//	JoinRAObject joinN = new JoinRAObject();
					while(joinItr.hasNext()){
						join = joinItr.next();
						BaseRAObject relationN = null;
						if(join.getRightItem() instanceof Table){
							relationN = new RelationRAObject();
							relationN.operator = RAOperator.RELATION;
							((RelationRAObject)relationN).tablename = ((Table)join.getRightItem()).getName();
							((RelationRAObject)relationN).alias = join.getRightItem().getAlias();
						}else{
							if(join.getRightItem() instanceof SubSelect){
								if(((SubSelect) join.getRightItem()).getSelectBody() instanceof PlainSelect)
									relationN = getTreeHeadFromSubSelect((PlainSelect)((SubSelect) join.getRightItem()).getSelectBody(),join.getRightItem().getAlias());
								else if(((SubSelect) join.getRightItem()).getSelectBody() instanceof Union)
									relationN = getTreeHeadFromUnion((Union)((SubSelect) join.getRightItem()).getSelectBody(),join.getRightItem().getAlias());
							}
						}
						JoinRAObject newJoin = new JoinRAObject();
						newJoin.leftChild = joinRAObject;
						newJoin.rightChild = relationN;
						if(null!=join.getOnExpression())
							newJoin.onExp = join.getOnExpression();
						joinRAObject.parent = newJoin;
						relationN.parent = newJoin;
						joinRAObject = newJoin;
					}
					if(currHead.leftChild==null)
						currHead.leftChild=joinRAObject;
					else
						currHead.rightChild=joinRAObject;
					joinRAObject.parent=currHead;



				}else{
					if(currHead.leftChild==null)
						currHead.leftChild=relation1;
					else
						currHead.rightChild=relation1;
					relation1.parent=currHead;


				}
			}
		}
		/*	Stack<BaseRAObject> operators = new Stack<BaseRAObject>();
		getStackFromRATree(operators,treeHead);
		while(!operators.isEmpty()){
			BaseRAObject op = operators.pop();
			System.out.println(op.operator);
		}*/
		return treeHead;

	}


	public BaseRAObject getTreeHeadFromSubSelect(PlainSelect stmt, String alias){

		BaseRAObject treeHead = new BaseRAObject();
		BaseRAObject currHead = new BaseRAObject();
		if(null!=stmt){
			if(stmt.getOrderByElements()!=null)
			{
				OrderByRAObject orderObj = new OrderByRAObject();
				orderObj.orderByItems = stmt.getOrderByElements();
				orderObj.operator=RAOperator.ORDER_BY;
				orderObj.parent=currHead;
				if(currHead.leftChild==null)
					currHead.leftChild = orderObj;
				else
					currHead.rightChild = orderObj;
				currHead = orderObj;
				treeHead=currHead;
				
			}
			//Projection
			
			if(null!=stmt.getSelectItems()){
				//Projection Items
				List<SelectItem> selectItems = stmt.getSelectItems();
				boolean aggr=false;
				boolean normal=false;
				for(SelectItem s : selectItems){
					if(s instanceof SelectExpressionItem){
						if(((SelectExpressionItem) s).getExpression() instanceof Function)
						{
							aggr=true;
						}
						else
						{
							normal=true;
						}
					}
				}
				if((aggr && normal) || (null!=stmt.getGroupByColumnReferences()) )
				{
						
					GroupByRAObject gpBy = new GroupByRAObject(stmt.getGroupByColumnReferences());
					gpBy.operator = RAOperator.GROUP_BY;
					gpBy.items=stmt.getSelectItems();
					gpBy.alias=alias;
					gpBy.parent = currHead;
					if(currHead.leftChild==null)
						currHead.leftChild = gpBy;
					else
						currHead.rightChild = gpBy;
					currHead = gpBy;
					treeHead=currHead;

				} 
				else if(aggr && !normal)
				{
					
					AggregateRAObject aggrObj =new AggregateRAObject();
					aggrObj.operator = RAOperator.AGGREGATE;
					aggrObj.parent = currHead;
					aggrObj.alias=alias;
					if(currHead.leftChild==null)
						currHead.leftChild = aggrObj;
					else
						currHead.rightChild = aggrObj;
					currHead = aggrObj;
					treeHead=currHead;
				}
				else if(!aggr && normal)
				{
					Limit limit = stmt.getLimit();
					ExProjectRAObject extProj = new ExProjectRAObject();
					extProj.limit=limit;
					extProj	.parent = currHead;
					extProj.operator = RAOperator.EXTENDED_PROJECT;
					extProj.items = stmt.getSelectItems();
					extProj.alias=alias;
					currHead.leftChild = extProj;
					currHead = extProj;
					treeHead=currHead;
				}
			}
			//Selection
			if(null!=stmt.getWhere()){
				WhereRAObject selectOb = new WhereRAObject(currHead);
				selectOb.operator = RAOperator.SELECT;
				selectOb.parent = currHead;
				selectOb.exp = stmt.getWhere();
				if(currHead.leftChild==null)
					currHead.leftChild = selectOb;
				else
					currHead.rightChild = selectOb;
				currHead = selectOb;
			}
			
			//Joins: Create a Left Deep Tree
			if(null!=stmt.getFromItem()){
				//To be returned
				BaseRAObject relationHead = null;
				//Deepest Node
				BaseRAObject relation1 = null;
				if(stmt.getFromItem() instanceof Table){
					relation1 = new RelationRAObject();
					relation1.operator = RAOperator.RELATION;
					((RelationRAObject)relation1).tablename = ((Table)stmt.getFromItem()).getName();
					((RelationRAObject)relation1).alias=stmt.getFromItem().getAlias();
				}
				
				else if(stmt.getFromItem() instanceof SubSelect){
					PlainSelect s = ((PlainSelect)((SubSelect)stmt.getFromItem()).getSelectBody());
					relation1=getTreeHeadFromSubSelect(s,stmt.getFromItem().getAlias());
				}
				if(null!=stmt.getJoins()){
					List<Join> joins = stmt.getJoins();
					Iterator<Join> joinItr = joins.iterator();
					Join join = joinItr.next();
					JoinRAObject joinRAObject = new JoinRAObject();
					relation1.parent = joinRAObject;
					joinRAObject.leftChild = (BaseRAObject)relation1;

					BaseRAObject relation2 = null;
					if(join.getRightItem() instanceof Table){
						relation2= new RelationRAObject();
						relation2.operator = RAOperator.RELATION;
						relation2.parent = joinRAObject;
						((RelationRAObject)relation2).tablename = ((Table)join.getRightItem()).getName();
						((RelationRAObject)relation2).alias = join.getRightItem().getAlias();
					}else if(join.getRightItem() instanceof SubSelect){
						if(((SubSelect) join.getRightItem()).getSelectBody() instanceof PlainSelect)

							relation2 = getTreeHeadFromSubSelect((PlainSelect)((SubSelect) join.getRightItem()).getSelectBody(),join.getRightItem().getAlias());
						else if(((SubSelect) join.getRightItem()).getSelectBody() instanceof Union)
							relation2 = getTreeHeadFromUnion((Union)((SubSelect) join.getRightItem()).getSelectBody(),join.getRightItem().getAlias());
					}
					joinRAObject.rightChild = (BaseRAObject)relation2;
					if(null!=join.getOnExpression())
						joinRAObject.onExp = join.getOnExpression();
					//	JoinRAObject joinN = new JoinRAObject();
					while(joinItr.hasNext()){
						join = joinItr.next();
						BaseRAObject relationN = null;
						if(join.getRightItem() instanceof Table){
							relationN = new RelationRAObject();
							relationN.operator = RAOperator.RELATION;
							((RelationRAObject)relationN).tablename = ((Table)join.getRightItem()).getName();
							((RelationRAObject)relationN).alias = join.getRightItem().getAlias();
						}else{
							if(join.getRightItem() instanceof SubSelect){
								if(((SubSelect) join.getRightItem()).getSelectBody() instanceof PlainSelect)
									relationN = getTreeHeadFromSubSelect((PlainSelect)((SubSelect) join.getRightItem()).getSelectBody(),join.getRightItem().getAlias());
								else if(((SubSelect) join.getRightItem()).getSelectBody() instanceof Union)
									relationN = getTreeHeadFromUnion((Union)((SubSelect) join.getRightItem()).getSelectBody(),join.getRightItem().getAlias());
							}
						}
						JoinRAObject newJoin = new JoinRAObject();
						newJoin.leftChild = joinRAObject;
						newJoin.rightChild = relationN;
						if(null!=join.getOnExpression())
							newJoin.onExp = join.getOnExpression();
						joinRAObject.parent = newJoin;
						relationN.parent = newJoin;
						joinRAObject = newJoin;
						if(currHead.leftChild==null)
							currHead.leftChild=joinRAObject;
						else
							currHead.rightChild=joinRAObject;
						joinRAObject.parent=currHead;

					}

				}else{
					if(currHead.leftChild==null)
						currHead.leftChild=relation1;
					else
						currHead.rightChild=relation1;
					relation1.parent=currHead;


				}
			}
		}
		/*Stack<BaseRAObject> operators = new Stack<BaseRAObject>();
		getStackFromRATree(operators,treeHead);
		while(!operators.isEmpty()){
			BaseRAObject op = operators.pop();
			System.out.println(op.operator);
		}*/
		return treeHead;



	}
	/// TODO : NEED TO USE ALIAS FOR SUBSELECT!!!!!
	public BaseRAObject getTreeHeadFromUnion(Union selBody, String alias){

		UnionRAObject unionRAObject = new UnionRAObject();
		unionRAObject.operator = RAOperator.UNION;
		List<PlainSelect> plainSelects = ((Union)selBody).getPlainSelects();
		for(PlainSelect stmt:plainSelects){
			unionRAObject.statementHeads.add(getTreeHeadFromSelect(stmt));
		}
		return unionRAObject;


	}

}
