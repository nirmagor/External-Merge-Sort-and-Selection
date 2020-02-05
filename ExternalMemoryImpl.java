package edu.hebrew.db.external;

import java.io.*;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.function.ToDoubleBiFunction;

public class ExternalMemoryImpl implements IExternalMemory {


	@Override
	public void sort(String in, String out, int colNum, String tmpPath) {

//		long timeBefore = System.nanoTime();
		int m = 1000;
		int sizeOfBlock = 20000;
		try {
			//Step 1
			ArrayList<String> oldFiles = writeToFiles(m, sizeOfBlock, colNum, tmpPath, in, 0, null);
			//Step 2
			step2(out, oldFiles, tmpPath, m, colNum);
//			int fileNum = 0;
//			File outfile = new File(out);
//
//			if(oldFiles.size() == 1){
//				BufferedWriter bw  =  new BufferedWriter(new FileWriter(outfile));
//				BufferedReader br = new BufferedReader(new FileReader(oldFiles.get(0)));
//				String currLine = br.readLine();
//				while (currLine != null){
//
//					bw.write(currLine + "\n");
//
//					currLine = br.readLine();
//				}
//				bw.close();
//
//
//			}
//			while (oldFiles.size() > 1){
//                int newFilesSize = (int)Math.ceil((double)oldFiles.size()/(m-1)); //Changed by Nir
//				ArrayList<String> newFiles = new ArrayList<String>();
//				for (int round = 0; round <newFilesSize; round++) {
//					File currentFile = File.createTempFile("file"+Integer.toString(fileNum), null,new File(tmpPath));
//					currentFile.deleteOnExit();
//					newFiles.add(currentFile.getPath());
//					BufferedWriter bw;
//					bw = (newFilesSize == 1) ?  new BufferedWriter(new FileWriter(outfile)) :
//							new BufferedWriter(new FileWriter(currentFile)) ; // final file case handling
//					ArrayList<SuperBuffer> superBuffers = initializer(round, oldFiles, m, colNum);
//					merge(bw, superBuffers);
//					fileNum++;
//					bw.close();
//				}
//				for (String path: oldFiles) {
//					new File(path).delete();
//				}
//				oldFiles = newFiles;
//			}
		}
		catch (IOException e ){
			//ToDo handle exceptions
		}
//		long timeAfter = System.nanoTime();
//		long totalInNano = timeAfter - timeBefore;
//		long totalInSec = (long) ((double)totalInNano/(1000000*1000));
//		System.out.println(totalInSec);
	}


	@Override
	public void select(String in, String out, int colNumSelect, String substrSelect, String tmpPath) {
		try{
			BufferedReader br = new BufferedReader(new FileReader(in));
			BufferedWriter bw = new BufferedWriter((new FileWriter(out)));
			String curreLine = br.readLine();
			while (curreLine != null){
				String relevantPart =(colNumSelect == 1) ?  curreLine.substring(0,20)
						: curreLine.substring(20 * (colNumSelect - 1) + 1, 20 * (colNumSelect - 1) + 21);
				if(relevantPart.contains(substrSelect)){
					bw.write(curreLine + "\n");
				}
				curreLine = br.readLine();
			}
			br.close();
			bw.close();

		}
		catch (IOException e){
			// TODO handle this
		}

	}


	@Override
	public void sortAndSelectEfficiently(String in, String out, int colNumSort,
			String tmpPath, int colNumSelect, String substrSelect) {
		int m = 1000;
		int sizeOfBlock = 20000;
		try {
			//Step 1
			ArrayList<String> oldFiles = writeToFiles(m, sizeOfBlock, colNumSort, tmpPath, in, colNumSelect, substrSelect);
			//Step 2
			step2(out, oldFiles, tmpPath, m, colNumSort);
		}
		catch (IOException e ){
			//ToDo handle exceptions
		}
	}


	ArrayList<SuperBuffer> initializer(int round, ArrayList<String> oldFiles, int m, int colNum) throws IOException {
		ArrayList<SuperBuffer> superBuffers = new ArrayList<SuperBuffer>();
		for (int j = 0; j < m-1; j++) {
			int calcI = (round * (m - 1));
			if(calcI + j >= oldFiles.size()) { break; }
			String filePath = oldFiles.get(calcI+j);
			BufferedReader br = new BufferedReader(new FileReader(filePath));
			superBuffers.add(new SuperBuffer(calcI + j, br, colNum));
		}
		return superBuffers;
	}


	private void merge(BufferedWriter bw, ArrayList<SuperBuffer> data) throws IOException {
		Comparator<SuperBuffer> comparator = new SuperBufferComparator();
		PriorityQueue<SuperBuffer> queueOfBuffers = new PriorityQueue<SuperBuffer>(comparator);

		queueOfBuffers.addAll(data);
		while (queueOfBuffers.size() > 0){
			SuperBuffer first = queueOfBuffers.poll();
			bw.write(first.currLine + "\n");
			first.next();
			if(first.currLine != null){
				queueOfBuffers.add(first);
			}
		}
	}

	private ArrayList<String> writeToFiles(int m, int sizeOfBlock, int sortBy, String tmpPath, String in, int
			colNumSelect, String subStringSelect)throws IOException{
		ArrayList<String> filesArray = new ArrayList<String>();
		BufferedReader br = new BufferedReader(new FileReader(in));


//		PriorityQueue<String> queueOfLines = new PriorityQueue<String>(new lineComparator(sortBy));
		String currLine = br.readLine();
		int sizeOfRow = (currLine.length()) * 2; // number of bytes in a line (mult by 2 for each char in the line)
		int numOfRowsInBlock = (int)Math.ceil(((double)sizeOfBlock / sizeOfRow));
		String[] arrayOfLines = new String[numOfRowsInBlock*m];
		int filesCounter = 0;
		while ((currLine) != null){

			for (int i = 0; i < numOfRowsInBlock*m ; i++) {

				if(colNumSelect != 0){
					String relevantPart =(colNumSelect == 1) ?  currLine.substring(0,20)
							: currLine.substring(20 * (colNumSelect - 1) + 1, 20 * (colNumSelect - 1) + 21);
					if(!relevantPart.contains(subStringSelect)){
						currLine = br.readLine();


					}
					else {
						arrayOfLines[i] = currLine;
//						queueOfLines.add(currLine);
						currLine = br.readLine();
					}
					if(currLine == null){
						break;
					}
				}
				else  {
					arrayOfLines[i] = currLine;
//					queueOfLines.add(currLine);
					currLine = br.readLine();
					if (currLine == null) {
						break;
					}
				}
			}

			Arrays.sort(arrayOfLines, new lineComparator(sortBy));

			File temp = File.createTempFile("begfile"+Integer.toString(filesCounter), null,new File(tmpPath));
			temp.deleteOnExit();
//			BufferedWriter bw = new BufferedWriter(new FileWriter(filesArray.get(filesCounter)));
			BufferedWriter bw = new BufferedWriter(new FileWriter(temp));
//			String lineToWrite = queueOfLines.poll();
			String lineToWrite;
			boolean didIWrite = false;
			for (int i = 0; i < numOfRowsInBlock * m; i++) {
				lineToWrite = arrayOfLines[i];
				if (lineToWrite  != null){
					bw.write(lineToWrite + "\n");
					didIWrite = true;
//					lineToWrite = queueOfLines.poll();
				}
			}
			if(didIWrite) {
				filesArray.add(temp.getPath());
			}

			bw.close();
			arrayOfLines = new String[numOfRowsInBlock*m];
//			queueOfLines.clear();
			filesCounter++;
		}
		return filesArray;
	}


	public void step2(String out, ArrayList<String> oldFiles, String tmpPath, int m, int colNum) throws IOException {
		int fileNum = 0;
		File outfile = new File(out);

		if(oldFiles.size() <= 1){
			String currLine = null;
			BufferedWriter bw  =  new BufferedWriter(new FileWriter(outfile));
			BufferedReader br = null;
			if(oldFiles.size()>0) {
				br = new BufferedReader(new FileReader(oldFiles.get(0)));
				currLine = br.readLine();
			}
			while (currLine != null){

				bw.write(currLine + "\n");

				currLine = br.readLine();
			}
			bw.close();
		}
		while (oldFiles.size() > 1){
			int newFilesSize = (int)Math.ceil((double)oldFiles.size()/(m-1)); //Changed by Nir
			ArrayList<String> newFiles = new ArrayList<String>();
			for (int round = 0; round <newFilesSize; round++) {
				File currentFile = File.createTempFile("file"+Integer.toString(fileNum), null,new File(tmpPath));
				currentFile.deleteOnExit();
				newFiles.add(currentFile.getPath()); // Changed by Nir
				BufferedWriter bw;
				bw = (newFilesSize == 1) ?  new BufferedWriter(new FileWriter(outfile)) :
						new BufferedWriter(new FileWriter(currentFile)) ; // final file case handling
				ArrayList<SuperBuffer> superBuffers = initializer(round, oldFiles, m, colNum);
				merge(bw, superBuffers);
				fileNum++;
				bw.close();
			}
			for (String path: oldFiles) {
				new File(path).delete();
			}
			oldFiles = newFiles;
		}
	}
}


class SuperBuffer{
	public int index;
	public BufferedReader buff;
	public String currLine;
	public String col;
    public int colNumber;


	SuperBuffer(int ind, BufferedReader buffer, int colNum) throws IOException {
		index = ind;
		buff = buffer;
		currLine = buffer.readLine();
		colNumber = colNum;
		if(currLine != null){
			String[] split = currLine.split(" ");
			col = split[colNum -1];
		}
	}


	public void next() throws IOException {
		currLine = buff.readLine();
		if(currLine != null){
			String[] split = currLine.split(" ");
			col = split[colNumber -1];
		}
	}
}


class SuperBufferComparator implements Comparator<SuperBuffer>{
	@Override
	public int compare(SuperBuffer s1, SuperBuffer s2){
//		if(s1.col == null )
		return s1.col.compareTo(s2.col);
	}
}


class lineComparator implements Comparator<String>
{
	public int col;
	lineComparator(int sortBy){
		col = sortBy;
	}
	@Override
	public int compare(String s1, String s2){
		if (s1 == null && s2 == null) {
			return 0;
		}
		if (s1 == null) {
			return 1;
		}
		if (s2 == null) {
			return -1;
		}

		String col1 =(col == 1) ?  s1.substring(0,20)
				: s1.substring(20 * (col - 1) + 1, 20 * (col - 1) + 21);
		String col2 =(col == 1) ?  s2.substring(0,20)
				: s2.substring(20 * (col - 1) + 1, 20 * (col - 1) + 21);



//		String[] split1 = s1.split(" ");
//		String col1 = split1[col -1];
//		String[] split2 = s2.split(" ");
//		String col2 = split2[col -1];
		return col1.compareTo(col2);
	}
}

