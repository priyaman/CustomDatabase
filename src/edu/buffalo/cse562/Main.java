package edu.buffalo.cse562;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.statement.create.table.CreateTable;
import edu.buffalo.cse562.globals.GlobalConstants;
import edu.buffalo.cse562.utils.statsCollection;

public class Main {

	public static void main(String[] args) {
		GlobalConstants.PREFIX_DATA_PATH = args[1] + GlobalConstants.SLASH; 
		SqlReader sqlReader = new SqlReader();
		
		List<File> sqlFiles = new ArrayList<File>();
		ArrayList<File> createFiles = new ArrayList<File>();
		//taking input from command line
		for(int i = 0 ; i<args.length;i++){
			if(args[i].equals("--data")){
				GlobalConstants.PREFIX_DATA_PATH = args[i+1] + GlobalConstants.SLASH;
				i++;
			}
			else if(args[i].equals("--db")){
				i++;
				GlobalConstants.PREFIX_DB_PATH = args[i] + GlobalConstants.SLASH;
			}
			else if(args[i].equals("--load")){
				i++;
				while(i<args.length)
				{
					createFiles.add(new File(args[i]));
					i++;
				}
				sqlReader.parseSqlFiles(createFiles,GlobalConstants.LOAD_PHASE);
			}
			/*else if(args[i].equals("--swap")){
				GlobalConstants.HAS_SWAP = false;//true; //HARD CODE TO AVOID EXT SORT
				GlobalConstants.SWAP_DIR = args[i+1] + GlobalConstants.SLASH;
				i++;
			}*/
			else{
				sqlFiles.add(new File(args[i]));
			}
		}
		if(sqlFiles.size()>0)
		sqlReader.parseSqlFiles(sqlFiles, GlobalConstants.QUERY_PHASE);
		
	}

}
