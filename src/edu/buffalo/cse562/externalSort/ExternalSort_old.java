package edu.buffalo.cse562.externalSort;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LeafValue.InvalidLeaf;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import edu.buffalo.cse562.beans.Schema;
import edu.buffalo.cse562.beans.Tuple;
import edu.buffalo.cse562.globals.GlobalConstants;
import edu.buffalo.cse562.operators.FileReaderOperator;
import edu.buffalo.cse562.sqlparser.ColDetails;
import edu.buffalo.cse562.utils.DateComparator;
import edu.buffalo.cse562.utils.DoubleComparator;
import edu.buffalo.cse562.utils.IntLongComparator;
import edu.buffalo.cse562.utils.StringComparator;

public class ExternalSort_old {
	private Map<String,Schema> relations;
	private Tuple[][] buffers;
	
	public ExternalSort_old(Map<String,Schema> relations){
		this.relations = relations;
	}
	public int getTupleReadCount(Schema schema){
		return (int) ( GlobalConstants.BLOCK_SIZE/schema.tupleSize);
	}
	
	public void externalSort(String filename, ColDetails colDetailsToSortOn){
		Schema schema = relations.get(filename);
		int tupleCount = getTupleReadCount(schema);
		buffers = new Tuple[GlobalConstants.NUM_BLOCKS][tupleCount];	
		int currBlock = 0;
		int currTuple = 0;
		
		FileReaderOperator fOpr = new FileReaderOperator(schema);
		Long fileSize = fOpr.calcAndGetFileSize();
		//For Pass 0
		int passNo = 0;
		int sortedRuns = (int) Math.ceil(((double)fileSize/((double)GlobalConstants.BLOCK_SIZE)/(double)GlobalConstants.NUM_BLOCKS)) ;
		String sortDataType = colDetailsToSortOn.colDef.getColDataType().getDataType();
		for(int run=0;run<sortedRuns;run++){
			while(fOpr.hasNext() && currBlock < GlobalConstants.NUM_BLOCKS){
				while(fOpr.hasNext() && currTuple < tupleCount){
					Tuple tupleLeaves = fOpr.getNext();
					if(!fOpr.hasNext())
						break;
					buffers[currBlock][currTuple] = tupleLeaves;
					currTuple++;
				}
				
			Tuple[] currBuffer = null;	
			if(currTuple==tupleCount)	
				currBuffer = buffers[currBlock];
			else{
				currBuffer = new Tuple[currTuple];
				for(int ctCtr=0;ctCtr<currTuple;ctCtr++){
					currBuffer[ctCtr] = buffers[currBlock][ctCtr];
				}
			}
			if(sortDataType.equals(GlobalConstants.CHAR_TYPE) || sortDataType.equals(GlobalConstants.VARCHAR_TYPE)||
					sortDataType.equals(GlobalConstants.STRING_TYPE)){
				//Fill in with comparators
				Arrays.sort(currBuffer, new StringComparator(schema.colIdxMap.get(colDetailsToSortOn.colDetails.getColumnName())));
			}
			else if (sortDataType.equals(GlobalConstants.DOUBLE_TYPE)|| sortDataType.equals(GlobalConstants.FLOAT_TYPE)){
				Arrays.sort(currBuffer, new DoubleComparator(schema.colIdxMap.get(colDetailsToSortOn.colDetails.getColumnName())));
			}
			else if (sortDataType.equals(GlobalConstants.DATE_TYPE)){
				Arrays.sort(currBuffer, new DateComparator(schema.colIdxMap.get(colDetailsToSortOn.colDetails.getColumnName())));
			}
			else if (sortDataType.equals(GlobalConstants.INTEGER_TYPE) ||sortDataType.equals(GlobalConstants.LONG_TYPE)){
				Arrays.sort(currBuffer, new IntLongComparator(schema.colIdxMap.get(colDetailsToSortOn.colDetails.getWholeColumnName())));
			}
			
			try {
				BufferedWriter fWrite = new BufferedWriter(new FileWriter(new File(GlobalConstants.SWAP_DIR+GlobalConstants.SLASH+"alpha"+ filename + "<>" +passNo + "<>" +currBlock+ "<>"+ run + GlobalConstants.DAT_SUFFIX )));
				for(int i=0;i<currBuffer.length;i++){
					StringBuffer strBuf = new StringBuffer();
					for(int j=0;j<currBuffer[i].tupleItems.length;j++){
							if(currBuffer[i].tupleItems[j] instanceof LongValue){
								strBuf.append(currBuffer[i].tupleItems[j].toLong() + "|");
							}
							else if(currBuffer[i].tupleItems[j] instanceof DoubleValue){
								strBuf.append(currBuffer[i].tupleItems[j].toDouble() + "|");
							}
							else if(currBuffer[i].tupleItems[j] instanceof DateValue){
								strBuf.append(currBuffer[i].tupleItems[j].toString() + "|");
							}
							else if(currBuffer[i].tupleItems[j] instanceof StringValue){
								strBuf.append(currBuffer[i].tupleItems[j].toString() + "|");
							}
							else{
								System.out.println("You forgot a datatype.");
							}
					}
					strBuf.append("\n");
					fWrite.write(strBuf.toString());
				}
				fWrite.close();
			} catch (InvalidLeaf | IOException e) {
				e.printStackTrace();
			}
			currBlock++;
			currTuple = 0;
			
		}
		currBlock =0;	
/*		for(int i=0;i<currBuffer.length;i++){
			for(int j=0;j<currBuffer[i].tupleItems.length;j++){
				try {
					System.out.print(currBuffer[i].tupleItems[j].toLong() + "|");
				} catch (InvalidLeaf e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			System.out.println();
		}
	*/
		
		}
		//System.gc();
		sortMerge(filename, schema, colDetailsToSortOn);
	}
	//Files Stored
	//filename + "<>" +passNo + "<>" +currBlock+ "<>"+ run
	void sortMerge(String filename, Schema currSchema, ColDetails colDetailsToBeSortOn){
		//PASS >0
		int join1 = 0;
		int currPass = 1;
		int currFileReader = 0;
		int sortBlocks = GlobalConstants.NUM_BLOCKS-1;
		File swapFolder = new File(GlobalConstants.SWAP_DIR);
		int tupleCount = getTupleReadCount(currSchema);
		FileReaderOperator[] fileOperators = new FileReaderOperator[sortBlocks];
		int colIdx= currSchema.colIdxMap.get(colDetailsToBeSortOn.colDetails.getWholeColumnName());
		String sortDataType = colDetailsToBeSortOn.colDef.getColDataType().getDataType();
		Comparator<Tuple> comparator = null;
		if(sortDataType.equals(GlobalConstants.CHAR_TYPE) || sortDataType.equals(GlobalConstants.VARCHAR_TYPE)||
				sortDataType.equals(GlobalConstants.STRING_TYPE)){
			//Fill in with comparators
			comparator = new StringComparator(colIdx);
		}
		else if (sortDataType.equals(GlobalConstants.DOUBLE_TYPE)|| sortDataType.equals(GlobalConstants.FLOAT_TYPE)){
			comparator = new DoubleComparator(colIdx);
		}
		else if (sortDataType.equals(GlobalConstants.DATE_TYPE)){
			comparator = new DateComparator(colIdx);
		}
		else if (sortDataType.equals(GlobalConstants.INTEGER_TYPE) ||sortDataType.equals(GlobalConstants.LONG_TYPE)){
			comparator = new IntLongComparator(colIdx);
		}
		
		
		File[] swapFiles = swapFolder.listFiles();
		//MAKE A LIST OF REQUIRED FILES
		List<String> passFiles = new ArrayList<String>();
		for(File file:swapFiles){
			if(file.getName().startsWith("alpha"+filename+"<>"+(currPass-1)+"")){
				if(file.getName().endsWith(GlobalConstants.DAT_SUFFIX))
				passFiles.add(file.getName().substring(0, file.getName().length()-4));
			}else
				continue;
		}
		
		
		for(String passFileName:passFiles){
			for(int sortedRuns=0;sortedRuns<swapFiles.length/sortBlocks;sortedRuns++){
				while(currFileReader < sortBlocks){
					//CREATE FILE POINTERS FOR FIRST NUM_BLOCK-1 FILES
					currSchema.table.setName(passFileName);
					fileOperators[currFileReader] = new FileReaderOperator(currSchema, GlobalConstants.SWAP_DIR);
					currFileReader++;
				}

				//FILL THE BUFFERS
				for(int i=0;i<sortBlocks;i++){
					for(int j=0;j<tupleCount;j++){
						Tuple tupleLeaves = fileOperators[i].getNext();
						buffers[i][j] = tupleLeaves;
					}
				}
				//MERGE
			
				BufferedWriter fWrite = null;
				try {
					fWrite = new BufferedWriter(new FileWriter(new File(GlobalConstants.SWAP_DIR+GlobalConstants.SLASH+"merge"+ filename + "<>" +currPass + "<>" +join1)));
			
					int i=0,j=0,k=0,l=0;
					int outputNum = 0;
					Tuple one = buffers[0][0];
					Tuple two = buffers[1][0];
					Tuple three = buffers[2][0];
					Tuple four = buffers[3][0];
					Tuple min = one;
					int whichMin = 0;
					while(i<tupleCount && j<tupleCount && k<tupleCount && l<tupleCount ){
						if(comparator.compare(min, two) > 0){
							min = two;
							whichMin = 1;
						}
						if(comparator.compare(min, three) > 0){
							min = two;
							whichMin = 2;
						}
						if(comparator.compare(min, four) > 0){
							min = two;
							whichMin = 3;
						}
						if(outputNum < tupleCount){
							buffers[sortBlocks][outputNum++] = min;
							if(whichMin==0)
								i++;
							else if(whichMin==1)
								j++;
							else if(whichMin==1)
								k++;
							else if(whichMin==1)
								l++;
						}else{
							try {
							
								for(int xx=0;xx<buffers[sortBlocks].length;xx++){
									StringBuffer strBuf = new StringBuffer();
									for(int yy=0;yy<buffers[sortBlocks][xx].tupleItems.length;yy++){
										if(buffers[sortBlocks][xx].tupleItems[j] instanceof LongValue){
											strBuf.append(buffers[sortBlocks][xx].tupleItems[j].toLong() + "|");
										}
										else if(buffers[sortBlocks][xx].tupleItems[j] instanceof DoubleValue){
											strBuf.append(buffers[sortBlocks][xx].tupleItems[j].toDouble() + "|");
										}
										else if(buffers[sortBlocks][xx].tupleItems[j] instanceof DateValue){
											strBuf.append(buffers[sortBlocks][xx].tupleItems[j].toString() + "|");
										}
										else if(buffers[sortBlocks][xx].tupleItems[j] instanceof StringValue){
											strBuf.append(buffers[sortBlocks][xx].tupleItems[j].toString() + "|");
										}
										else{
											System.out.println("You forgot a datatype.");
										}
								}
								strBuf.append("\n");
								fWrite.write(strBuf.toString());
								}
							}catch (InvalidLeaf | IOException e) {
								e.printStackTrace();
							}
						}
				}
				fWrite.close();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			//DONE MERGE AND WRITE
			//WE HAVE MORE FILES
			join1++;
			currFileReader = 0;
			}
		}
	}
}
