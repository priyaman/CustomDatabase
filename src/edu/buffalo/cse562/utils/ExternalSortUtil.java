package edu.buffalo.cse562.utils;


import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import edu.buffalo.cse562.beans.Schema;
import edu.buffalo.cse562.beans.Tuple;
import edu.buffalo.cse562.globals.GlobalConstants;
import edu.buffalo.cse562.sqlparser.ColDetails;

public class ExternalSortUtil {
	
	public static void getTupleSize(Schema schema){
		long tupleSize  = 0;
		Map<String, ColDetails> colDetails = schema.ColumnMap;
		for(Map.Entry<String, ColDetails> entry : colDetails.entrySet()){
			ColDetails detail = entry.getValue();
			ColumnDefinition  def = detail.colDef;
			ColDataType type =def.getColDataType();
			int factor = 1;
			if(type.getArgumentsStringList()!=null){
				factor = factor * Integer.parseInt(type.getArgumentsStringList().get(0).toString());
			}else{
				factor = 1;
			}
				
			tupleSize += factor*GlobalConstants.datatypeSizeMap.get(type.getDataType());
		}
		schema.tupleSize = tupleSize;
		
	}
	
	public static String getFileWritableTuple(Tuple tuple){
		StringBuffer strBuf = new StringBuffer();
		try{
			for(int j=0;j<tuple.tupleItems.length;j++){
					if(tuple.tupleItems[j] instanceof LongValue){
						strBuf.append(tuple.tupleItems[j].toLong() + GlobalConstants.PIPE);
					}
					else if(tuple.tupleItems[j] instanceof DoubleValue){
						strBuf.append(tuple.tupleItems[j].toDouble() + GlobalConstants.PIPE);
					}
					else if(tuple.tupleItems[j] instanceof DateValue){
						strBuf.append(tuple.tupleItems[j].toString() + GlobalConstants.PIPE);
					}
					else if(tuple.tupleItems[j] instanceof StringValue){
						strBuf.append(tuple.tupleItems[j].toString() + GlobalConstants.PIPE);
					}
					else{
						System.out.println("You forgot a datatype.");
					}
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		strBuf.append("\n");
		return strBuf.toString();
	}
	
	public static Comparator<Tuple> getComparatorFromDatatype(String sortDataType, int colIdx){
		if(sortDataType.equals(GlobalConstants.CHAR_TYPE) || sortDataType.equals(GlobalConstants.VARCHAR_TYPE)||
				sortDataType.equals(GlobalConstants.STRING_TYPE)){
			return new StringComparator(colIdx);
		}
		else if (sortDataType.equals(GlobalConstants.DOUBLE_TYPE)|| sortDataType.equals(GlobalConstants.FLOAT_TYPE)){
			return new DoubleComparator(colIdx);
		}
		else if (sortDataType.equals(GlobalConstants.DATE_TYPE)){
			return new DateComparator(colIdx);
		}
		else if (sortDataType.equals(GlobalConstants.INTEGER_TYPE) ||sortDataType.equals(GlobalConstants.LONG_TYPE)){
			return new IntLongComparator(colIdx);
		}
		else{
			System.err.println("Not a Valid Datatype");
			return null;
		}
	}
	 
	
}
