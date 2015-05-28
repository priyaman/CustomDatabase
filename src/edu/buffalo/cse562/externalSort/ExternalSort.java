package edu.buffalo.cse562.externalSort;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.LeafValue;
import edu.buffalo.cse562.beans.Schema;
import edu.buffalo.cse562.beans.Tuple;
import edu.buffalo.cse562.globals.GlobalConstants;
import edu.buffalo.cse562.interfaces.Operator;
import edu.buffalo.cse562.operators.FileReaderOperator;
import edu.buffalo.cse562.sqlparser.ColDetails;
import edu.buffalo.cse562.utils.ExternalSortUtil;
import edu.buffalo.cse562.utils.Utilities;


public class ExternalSort {
	//private Map<String,Schema> relations;
	Operator prevOperator;
	Schema schema;
	private Tuple[][] buffers;
	private int[] bufCounts;
	private int tupleCount;
	int sortBlocks = GlobalConstants.NUM_BLOCKS-1;;
	int sortColIdx;
	String sortDataType;
	String outputFilename;
	
	/*public ExternalSort(Map<String,Schema> relations){
		this.relations = relations;
	}*/
	public ExternalSort(Operator prevOperator, Schema schema){
		this.prevOperator = prevOperator;
		this.schema = schema;
	}
	
	public int getTupleReadCount(Schema schema){
		return (int) ( GlobalConstants.BLOCK_SIZE/schema.tupleSize);
	}
	private void initialize(){		
		this.outputFilename = GlobalConstants.SWAP_DIR + Utilities.getRandomString() + GlobalConstants.DAT_SUFFIX;
		Writer writer = null;

		try {
		    writer = new BufferedWriter(new OutputStreamWriter(
		          new FileOutputStream(outputFilename), "utf-8"));
		    Tuple line=null;
		    do{
		    	line = prevOperator.getNext(); 
		    	writer.write(ExternalSortUtil.getFileWritableTuple(prevOperator.getNext()));
		    }while(line!=null);
		} catch (IOException ex) {
		} finally {
		   try {writer.close();} catch (Exception ex) {}
		}
	}
	
	public String externalSort(ColDetails colDetailsToSortOn){
		String returnFilename = null;
		initialize();

		int prevSortedRuns = externalSortPass0(schema, outputFilename, colDetailsToSortOn);
		int N=0;
		int i = prevSortedRuns;
		while((i)!=0){
			N++;
			i=i/sortBlocks;
			}
		try{
			returnFilename = externalSortPassN(schema, outputFilename+"<>0", N, colDetailsToSortOn, prevSortedRuns);
		}catch(Exception e){
		//	System.out.println("Archana's code doesnt work");
			e.printStackTrace();
		}
		return returnFilename;
	}
	
	private void sortMerge(Tuple[][] buffers,BufferedWriter outWrite, int sortBlocks) throws IOException{
		int[] bufCount = new int[sortBlocks];
		boolean allBufferEmpty = false;
		Comparator<Tuple> comparator = ExternalSortUtil.getComparatorFromDatatype(sortDataType, sortColIdx);
		int whichMin=0;
		int outputCount=0;
		do{
			if(outputCount == tupleCount){
				//WRITE TO DISK
				StringBuffer strBuf = new StringBuffer();
				for(int i=0;i<tupleCount;i++){
					strBuf.append(ExternalSortUtil.getFileWritableTuple(buffers[sortBlocks][i]));
				}
				outWrite.write(strBuf.toString());
				outputCount = 0;
				whichMin = -1;
			}
			
			Tuple minTuple = null;//buffers[0][bufCount[0]];
			for(int i=0;i<sortBlocks;i++){
				if(bufCount[i]==tupleCount)
					continue;
				Tuple currTup = buffers[i][bufCount[i]];
				if(currTup==null)
					continue;
				if(minTuple==null){
					minTuple = currTup;
					whichMin = i;
				}
				
				if(comparator.compare(minTuple, currTup) > 0){
					minTuple = currTup;
					whichMin = i;
				}
			}
			if(whichMin==-1)
				return;
			buffers[sortBlocks][outputCount++] = minTuple;
			bufCount[whichMin]++;

			//CHECK IF ALL BUFFERS ARE EMPTY
			allBufferEmpty = true;
			for(int bufNo=0;bufNo<sortBlocks;bufNo++){
				if(bufCount[bufNo]==(tupleCount))
					allBufferEmpty= allBufferEmpty & true;
				else
					allBufferEmpty= allBufferEmpty & false;
			}	
		}while(!allBufferEmpty);
		if(outputCount!=0){
			//WRITE REST TO DISK
			StringBuffer strBuf = new StringBuffer();
			for(int i=0;i<outputCount;i++){
				strBuf.append(ExternalSortUtil.getFileWritableTuple(buffers[sortBlocks][i]));
			}
			outWrite.write(strBuf.toString());
			outputCount = 0;
		}
	}
	
	private String externalSortPassN(Schema schema, String prefixName, int N, ColDetails colDetailsToSortOn, int prevSortedRuns) throws IOException{
	//	System.out.println("totalPassNo:"+N);
		buffers = new Tuple[GlobalConstants.NUM_BLOCKS][tupleCount];
		//GET LIST OF FILES IN FOLDER
		File swapFolder = new File(GlobalConstants.SWAP_DIR);
		String returnFileName = null;
		//while(N!=0){
		for(int nn=1;nn<=N;nn++){
		//	System.out.println("PassNo:"+nn);
			int noSortedRuns = (int)Math.ceil((double)prevSortedRuns/(double)sortBlocks);
			File[] swapFiles = swapFolder.listFiles();
			List<String> passFiles = new ArrayList<String>();
			
			
			for(File file:swapFiles){
				if(file.getName().startsWith(prefixName)){
					if(file.getName().endsWith(GlobalConstants.DAT_SUFFIX))
					passFiles.add(file.getName().substring(0, file.getName().length()-4));
				}else
					continue;
			}
			Collections.sort(passFiles);
			Iterator<String> passFileItrs = passFiles.iterator();
			//CALCULATE JOINZ
			//TODO: LETS SEE WHAT ORDER THE FILES COME IN
			//schema.table.setName(passFiles.get(0));
			//FileReaderOperator tFOprs = new FileReaderOperator();//schema, GlobalConstants.SWAP_DIR,passFileItrs.next());
			//Long filesize = tFOprs.calcAndGetFileSize();
			int numFileReaders = 0;
			if(prevSortedRuns < sortBlocks){
				numFileReaders = prevSortedRuns;
			}else{
				numFileReaders = sortBlocks;
			}
			 
			FileReaderOperator[] fOprs = new FileReaderOperator[numFileReaders];
			for(int runNo=0;runNo<noSortedRuns;runNo++){
				//OUTPUT FILE
				returnFileName = schema.table.getName() + "<>" + nn + "<>" + runNo ; 
				BufferedWriter outWrite = new BufferedWriter(new FileWriter(new File(GlobalConstants.SWAP_DIR+GlobalConstants.SLASH + schema.table.getName() + "<>" + nn + "<>" + runNo + GlobalConstants.DAT_SUFFIX )));
				int usedBlocks =0;
				for(int bufNo=0;bufNo<numFileReaders && passFileItrs.hasNext();bufNo++){
					//OPEN FILE_OPR FOR EVERY BUFFER (SORT_BLOCKS)
					String file = passFileItrs.next();
					//System.out.println("buffer for > "+file);
					fOprs[bufNo] = new FileReaderOperator(schema, GlobalConstants.SWAP_DIR, file);
					usedBlocks++;
				}
				
				
				boolean allFilesEmpty = false;
				do{
					bufCounts = new int[usedBlocks];
					//CLEAR BUFFERS
					for(int bufNo=0;bufNo<GlobalConstants.NUM_BLOCKS;bufNo++){
						for(int tupNo=0;tupNo<tupleCount;tupNo++){
							buffers[bufNo][tupNo] = null;
						}
					}
					//FILL BUFFERS
					for(int bufNo=0;bufNo<usedBlocks/* && fOprs[bufNo]!=null && fOprs[bufNo].hasNext()*/;bufNo++){
						for(int tupNo=0;tupNo<tupleCount && fOprs[bufNo].hasNext();tupNo++){
							//System.out.println(">>>" + bufNo);
							Tuple tupleLeaves = fOprs[bufNo].getNext();
							//if(!fOprs[bufNo].hasNext())
							//	break;
							buffers[bufNo][tupNo] = tupleLeaves;
							bufCounts[bufNo]++;
						}
					}
					//DO SORT MERGE TILL ALL BUFFS ARE EMPTY
					sortMerge(buffers,outWrite,usedBlocks);
					allFilesEmpty = true;
					for(int temp=0;temp<usedBlocks;temp++){
						if(fOprs[temp].hasNext()){
							allFilesEmpty = false;//allFilesEmpty & true;
							break;
						}
						/*else{
							allFilesEmpty = allFilesEmpty & false;
							break;
						}*/
					}
				}while(!allFilesEmpty);
				for(int i=0;i<usedBlocks;i++){
					fOprs[i] = null;
				}
				outWrite.close();
			}
			
			
			
			prevSortedRuns = noSortedRuns;
			prefixName = schema.table.getName() + "<>" + (nn) ;
			//N--;
			
		}
		
		return returnFileName;
		
	}
	
	private int externalSortPass0(Schema schema, String filename, ColDetails colDetailsToSortOn){
		int passNo = 0;
		this.tupleCount = getTupleReadCount(schema);
		this.sortDataType = colDetailsToSortOn.colDef.getColDataType().getDataType();
		//GET NUM SORTED RUNS
		FileReaderOperator fOpr = new FileReaderOperator(schema);
		Long fileSize = fOpr.calcAndGetFileSize();
		int numSortedRuns = (int) Math.ceil(((double)fileSize/((double)GlobalConstants.BLOCK_SIZE)/(double)GlobalConstants.NUM_BLOCKS)) ;
		//INIT BUFFERS
		buffers = new Tuple[GlobalConstants.NUM_BLOCKS][tupleCount];
		bufCounts = new int[GlobalConstants.NUM_BLOCKS];
		int opFileNo = 0;
		for(int run=0;run<numSortedRuns;run++){
			//FILL BUFFERS
			for(int bufNo=0;bufNo<GlobalConstants.NUM_BLOCKS && fOpr.hasNext();bufNo++){
				for(int tupNo=0;tupNo<tupleCount&& fOpr.hasNext();tupNo++){
					//System.out.println(">>>" + bufNo);
					Tuple tupleLeaves = fOpr.getNext();
					//if(!fOpr.hasNext())
					//	break;
					buffers[bufNo][tupNo] = tupleLeaves;
					bufCounts[bufNo]++;
				}
			}
			//SORT BUFFERS
			Tuple[] currBuffer = null;
			for(int bufNo=0;bufNo<GlobalConstants.NUM_BLOCKS;bufNo++){
				if(bufCounts[bufNo]==tupleCount){
					currBuffer = buffers[bufNo];
				}else{
					currBuffer = new Tuple[bufCounts[bufNo]];
					for(int ctCtr=0;ctCtr<bufCounts[bufNo];ctCtr++){
						currBuffer[ctCtr] = buffers[bufNo][ctCtr];
					}
				}
				sortColIdx = schema.colIdxMap.get(colDetailsToSortOn.colDetails.getWholeColumnName());
				Comparator<Tuple> comparator = ExternalSortUtil.getComparatorFromDatatype(sortDataType, sortColIdx);
				Arrays.sort(currBuffer, comparator);
				
				
				//WRITE SORTED BUFFER TO FILE
				try{
					BufferedWriter fWrite = new BufferedWriter(new FileWriter(new File(GlobalConstants.SWAP_DIR+GlobalConstants.SLASH + filename + "<>" +passNo + "<>" + opFileNo++ + "<>" + GlobalConstants.DAT_SUFFIX )));
					StringBuffer toWrite = new StringBuffer();
					for(int tupNo=0;tupNo<bufCounts[bufNo];tupNo++){
						toWrite.append(ExternalSortUtil.getFileWritableTuple(currBuffer[tupNo]));
					}
					fWrite.write(toWrite.toString());
					fWrite.close();
				}catch(FileNotFoundException e){
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
				
			}
			//RESET BUF_COUNTS
			for(int i=0;i<GlobalConstants.NUM_BLOCKS;i++){
				bufCounts[i] = 0;
			}
			
		}
		
		return opFileNo;
	}
}
