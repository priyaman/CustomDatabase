package edu.buffalo.cse562.raobjects;

import java.util.HashMap;
import java.util.List;

import edu.buffalo.cse562.beans.Schema;
import edu.buffalo.cse562.sqlparser.ColDetails;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class AggregateRAObject extends BaseRAObject{
	public List<SelectItem> items;
	public Limit limit;
	public String alias;
	public HashMap<String,LeafValue> hm;
	public AggregateRAObject() {
	}
	
	public void createSchema(Schema schema)
	{
		this.inSchema=schema;
		//this.input=input;
		//this.items=selectItems;
		this.hm = new HashMap<String,LeafValue>();
		this.outSchema=new Schema();
		this.outSchema.table = new Table();
		if(alias!=null)
			this.outSchema.table.setName(alias);
		int count=0; //for index
		for(SelectItem sel : this.items)
		{
			if(sel instanceof AllColumns)
			{
				this.outSchema=this.inSchema;
				this.outSchema.table.setAlias(alias);
			}
			else if(sel instanceof SelectExpressionItem)
			{
				String colName="";
				ColDetails cd = new ColDetails();

				cd.colDetails= new Column();
				cd.colDef=new ColumnDefinition();

				if(((SelectExpressionItem) sel).getAlias() != null)
				{
					cd.colDetails.setColumnName(((SelectExpressionItem) sel).getAlias());
					cd.colDetails.setTable(this.outSchema.table);
					cd.colDef.setColumnName(((SelectExpressionItem) sel).getAlias());
					colName=((SelectExpressionItem) sel).getAlias();
					
				}
				else
				{	cd.colDetails.setColumnName(sel.toString());
				cd.colDef.setColumnName(sel.toString());
				cd.colDetails.setTable(this.outSchema.table);
				if(alias==null)
				{
					colName=sel.toString();
				}
				else
					colName=alias+"."+sel.toString();
				}
				this.outSchema.ColumnMap.put(colName, cd);
				this.outSchema.colIdxMap.put(colName.toUpperCase(), count);
				count++;
			}
			

		}


	}
}
