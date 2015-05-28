package edu.buffalo.cse562.operators;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import edu.buffalo.cse562.beans.Schema;
import edu.buffalo.cse562.beans.Tuple;
import edu.buffalo.cse562.globals.GlobalConstants;
import edu.buffalo.cse562.interfaces.Operator;
import edu.buffalo.cse562.sqlparser.ColDetails;

public class FileReaderOperator extends BaseOperator implements Operator {
	List<ColumnDefinition> colNames=null;
	BufferedReader fReader = null;
	// Scanner fReader = null;
	Long fileSize = null;
	boolean status = false;
	String dataPrefixPath = GlobalConstants.PREFIX_DATA_PATH;
	String line = null;
	String bufferedLine = null;
	boolean initFlag = true;

	public FileReaderOperator(Schema schema) {
		this.schema = schema;
		colNames = getColDefs(schema);
		this.reset();
	}

	public FileReaderOperator(Schema schema, String dataPrefixPath) {
		this.dataPrefixPath = dataPrefixPath;
		this.schema = schema;
		colNames = getColDefs(schema);
		this.reset();
	}

	public FileReaderOperator(Schema schema, String dataPrefixPath,
			String fileName) {
		this.dataPrefixPath = dataPrefixPath;
		this.schema = schema;
		colNames = getColDefs(schema);
		this.reset(fileName);
	}

	public Long calcAndGetFileSize() {
		InputStream is;
		try {
			is = new BufferedInputStream(new FileInputStream(dataPrefixPath
					+ schema.table.getName() + GlobalConstants.DAT_SUFFIX));

			try {
				boolean empty = true;
				int readChars = 0;
				byte[] c = new byte[1024];
				int count = 0;
				while ((readChars = is.read(c)) != -1) {
					empty = false;
					for (int i = 0; i < readChars; ++i) {
						if (c[i] == '\n') {
							++count;
						}
					}
				}
				return (long) ((count == 0 && !empty) ? 1 : count)
						* schema.tupleSize;
			} finally {
				is.close();
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public boolean hasNext() {
		if (line == null && bufferedLine == null && initFlag == false)
			this.status = false;
		// this.status = fReader.hasNext();
		return this.status;
	}

	@Override
	public Tuple getNext() {
		String line = null;
		try {
			// line = fReader.readLine();
			// if(fReader.hasNext())
			// line = fReader.nextLine();
			// else
			// return null;
			if (initFlag) {
				bufferedLine = fReader.readLine();
				//System.out.println(bufferedLine);
				initFlag = false;
			}
			line = fReader.readLine();
			if (line == null)
				status = false;
			//System.out.println(line);
		} catch (Exception e) {
			e.printStackTrace();
		}
		//Might mess up External Sort
		String[] data = null;
		if(bufferedLine!=null)
			data = bufferedLine.split("\\|");
		else
			return null;
		Tuple retTuple = new Tuple();
		retTuple.tupleItems = new LeafValue[data.length];


		for (int i = 0; i < data.length; i++) {
			String datatype = colNames.get(i).getColDataType().getDataType();
			if (datatype.equalsIgnoreCase(GlobalConstants.INTEGER_TYPE)
					|| datatype.equalsIgnoreCase(GlobalConstants.LONG_TYPE)) {
				retTuple.tupleItems[i] = new LongValue(data[i]);
				//retTuple.tupleItems[i] = new DoubleValue(data[i]);
			} else if (datatype.equalsIgnoreCase(GlobalConstants.STRING_TYPE)
					|| datatype.equalsIgnoreCase(GlobalConstants.VARCHAR_TYPE)
					|| datatype.equalsIgnoreCase(GlobalConstants.CHAR_TYPE)) {
				retTuple.tupleItems[i] = new StringValue(" "+data[i]+ " ");

				// ((StringValue)retVal[i]).setValue(((StringValue)retVal[i]).toString().replaceFirst("'",
				// ""));
				// retVal[i] = new StringValue(data[i].substring(1,
				// data[i].length()-1));
			} else if (datatype.equalsIgnoreCase(GlobalConstants.DATE_TYPE)) {
				retTuple.tupleItems[i] = new DateValue("'" + data[i] + "'");

			} else if (datatype.equalsIgnoreCase(GlobalConstants.DOUBLE_TYPE)) {
				retTuple.tupleItems[i] = new DoubleValue(data[i]);
			} else{
				System.out.println("Invalid datatype" + datatype.toString());
			}
		}
		if (!initFlag)
			bufferedLine = line;
		return retTuple;
	}

	List<ColumnDefinition> getColDefs(Schema schema)

	{
		List<ColumnDefinition> colDefs = new ArrayList<ColumnDefinition>();

		ColumnDefinition[] arr = new ColumnDefinition[schema.ColumnMap.size()];
		Iterator it = schema.ColumnMap.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, ColDetails> pair = (Entry<String, ColDetails>) it
					.next();

			int a = schema.colIdxMap.get(pair.getKey().toUpperCase());
			arr[a] = (pair.getValue().colDef);

		}

		for (int i = 0; i < arr.length; i++) {
			colDefs.add(arr[i]);
		}

		return colDefs;
	}

	@Override
	public void reset() {
		try {
			fReader = new BufferedReader(new FileReader(dataPrefixPath
					+ schema.table.getName() + GlobalConstants.DAT_SUFFIX));
			// fReader = new Scanner(new
			// FileInputStream(dataPrefixPath+schema.table.getName()+".dat"));
			this.status = true;
		} catch (FileNotFoundException e) {
			System.err.println("Error in reset File");
			e.printStackTrace();
		}

	}

	public void reset(String filename) {
		try {
			fReader = new BufferedReader(new FileReader(dataPrefixPath
					+ filename + GlobalConstants.DAT_SUFFIX));
			// fReader = new Scanner(new
			// FileInputStream(dataPrefixPath+filename+GlobalConstants.DAT_SUFFIX));
			this.status = true;
		} catch (FileNotFoundException e) {
			System.err.println("Error in reset File");
			e.printStackTrace();
		}

	}

}
