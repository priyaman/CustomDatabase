package edu.buffalo.cse562.utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;



public class createHistogram {

	Histogram h;

	ArrayList<Double> allData = new ArrayList<Double>();
	String filename;
	public createHistogram(String filename )
	{
		this.filename=filename;
		h = new Histogram();
		h.minMax = new double[2]; 
	}

	public static void main(String args[])
	{
		createHistogram ch = new createHistogram("/home/shweta/DB_PROJECT/200mb/CUSTOMER.dat");
		ch.getData(0);
		ch.create();
	}
	public void getData(int colIndex)
	{
		double min = Integer.MAX_VALUE;
		double max = Integer.MIN_VALUE;
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(filename));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String line=null;
		try {
			while((line =br.readLine())!=null)
			{
				double key = Double.parseDouble(line.split("\\|")[colIndex]);
				if(key>max)
					max=key;
				if(key<min)
					min=key;
				allData.add(key);
			}
			h.minMax[0]=min;
			h.minMax[1]=max;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}



	}
	public void create()
	{
		
		double sqroot = Math.sqrt(h.minMax[1] - h.minMax[0]);
		h.Histogram= new int[(int) (sqroot+1)];
		for(int i=0;i<allData.size();i++)
		{
			if(allData.get(i)==h.minMax[0])
			{
				h.Histogram[0]=h.Histogram[0]+1;
			}
			else
			{
				int index = (int) ((allData.get(i) - h.minMax[0])/sqroot);				
				h.Histogram[index]=h.Histogram[index]+1;
			}	
		}
		for(int i=0;i<h.Histogram.length;i++)
			System.out.println(h.Histogram[i]);
		System.out.println(h.Histogram.length);
	}

}
