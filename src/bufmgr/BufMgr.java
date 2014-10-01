package bufmgr;
import global.*;
import diskmgr.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Stack;

import global.PageId;


public class BufMgr {

	//instance variables
	Descriptor [] bufDescr;
	byte [][] bufpool ;
	HashMap<Integer, Integer> pg_frm;
	int numberOfFrames,numberOfPages,replaceType,busy,numberUnpinnedPages;
	Queue<Integer> FIFO,queue;
	
	
	public BufMgr(int numBufs, String replaceArg){
		bufpool = new byte [numBufs][GlobalConst.MAX_SPACE];	
		bufDescr = new Descriptor[numBufs];
		FIFO = new LinkedList<Integer>();
		queue = new LinkedList<Integer>();
		pg_frm = new HashMap<>();
		busy = 0;
		numberOfFrames = numBufs;
		numberOfPages = 0;
		numberUnpinnedPages = numberOfFrames;
		if(replaceArg.equals("FIFO") || replaceArg.equals("Clock")){
			replaceType = 1;
		}
		else if(replaceArg.equals("LRU")){
			replaceType = 2;
		}
		else if(replaceArg.equals("MRU")){
			replaceType = 3;
		}
		else if(replaceArg.equals("Love/Hate")){
			replaceType = 4;
		}
	}

	public void pinPage(PageId pgid, Page page, boolean emptyPage, boolean loved) throws BufferPoolExceededException, PagePinnedException, InvalidPageNumberException, FileIOException, IOException {
		if(pg_frm.containsKey(pgid.pid)){//the page already exists in the pool (RAM)
			int framenum = pg_frm.get(pgid.pid);
			if(bufDescr[framenum].pin_Count==0){
				int siz = queue.size();
				for(int i=0;i<siz;i++){
					if(queue.peek()==framenum) queue.poll();
					else queue.add(queue.poll());
				}
			}
			bufDescr[framenum].pin_Count++;
			bufDescr[framenum].user_counter++;
			page.setpage(bufpool[framenum]);
			
		}else{
			if(numberOfPages==numberOfFrames){//pool filled with pages
				
				if(queue.isEmpty()){
					// all pages are being used (ie. pin counts > 0)
					page = null;
					throw (new BufferPoolExceededException(null, ""));
				}else{
					//There Exists a victim
					int frameNum = -1;
					switch(replaceType){
						case 1: {
							boolean gotOne = false;
							for(int i=0;i<numberOfPages;i++){
								if(gotOne==false && bufDescr[FIFO.peek()].pin_Count==0){
									frameNum = FIFO.poll();
									gotOne = true;
								}else FIFO.add(FIFO.poll());
							}
							int sizeQ = queue.size();
							for(int i=0;i<sizeQ;i++){
								if(queue.peek()==frameNum){
									queue.poll();
								}else queue.add(queue.poll());
							}
						}break;
						// case 2 and case 3 might be flipped !!
						case 2: {
							frameNum = queue.poll();
						}break;
						case 3: {
							Stack<Integer> skpg = new Stack<>();
							for(int i=0;i<queue.size();i++){
								skpg.add(queue.poll());
							}
							frameNum = skpg.peek();
							for(int i=0;i<skpg.size();i++){
								queue.add(skpg.pop());
							}
							
						}break;
						//queue - stack LRU
						//stack - queue MRU
						case 4:{
							Stack<Integer> skpg = new Stack<>();
							boolean gotOne = false;
							int sizz = queue.size();
							for(int i=0;i<sizz;i++){
								if(gotOne==false && !bufDescr[queue.peek()].loved){
									frameNum = queue.poll();
									gotOne = true;
								}else{
									skpg.add(queue.poll());
								}
							}
							int sz = skpg.size();
							for(int i=0;i<sz;i++){
								if(gotOne==false && bufDescr[skpg.peek()].loved){
									frameNum = skpg.pop();
									gotOne = true;
								}else{
									queue.add(skpg.pop());
								}
							}
						}break;
						default: {
							System.out.println("you're in default of pin method  #2.2!!!");
						}break;
					}
					//handle the frameNum
					//save if dirty
					 if(bufDescr[frameNum].dirtybit){
						//write the page to be removed in disk
						
						Page tempo = new Page(bufpool[frameNum]);
						PageId pgid1 = new PageId(bufDescr[frameNum].pageid);
						
						pg_frm.remove(pgid1.pid);
						SystemDefs.JavabaseDB.write_page(pgid1, tempo);
						
					}
					//read from disk the required page
					
					Page temp = new Page();
					SystemDefs.JavabaseDB.read_page(pgid, temp);
					bufDescr[frameNum] = new Descriptor(pgid.pid, 1, false, 1, false);
					byte copied[] = temp.getpage();
					bufpool[frameNum] = temp.getpage();
					pg_frm.put(pgid.pid, frameNum);
					FIFO.add(frameNum);
					page.setpage(temp.getpage());
					
					//end handling the frameNum
				}
			}else{
				//pool has space
				Page tempe = new Page();
				SystemDefs.JavabaseDB.read_page(pgid, tempe);
				bufpool[numberOfPages] = tempe.getpage();
				bufDescr[numberOfPages] = new Descriptor(pgid.pid, 1, false, 1,false);
				FIFO.add(numberOfPages);
				page.setpage(tempe.getpage());
				pg_frm.put(pgid.pid, numberOfPages);
				numberOfPages++;
			}
			
		}

	}

	public void unpinPage(PageId pgid, boolean dirty, boolean loved)throws PageUnpinnedExcpetion, HashEntryNotFoundException{
		if(pg_frm.containsKey(pgid.pid)){
			int frameNum = pg_frm.get(pgid.pid);
			if(bufDescr[frameNum].pin_Count==0){
				throw new PageUnpinnedExcpetion(null, "PageUnpinnedExcpetion");
			}
			if(bufDescr[frameNum].pin_Count>0){
				bufDescr[frameNum].pin_Count--;
				if(bufDescr[frameNum].pin_Count==0){
					queue.add(frameNum);
				}
				bufDescr[frameNum].dirtybit |= (dirty);
				bufDescr[frameNum].loved |= (loved);
			}
		}else{
			throw (new HashEntryNotFoundException(null, ""));
		}
	}
	
	public PageId newPage(Page firstPage , int howmany) throws BufferPoolExceededException, OutOfSpaceException, InvalidRunSizeException, InvalidPageNumberException, FileIOException, DiskMgrException, IOException{
		PageId pidd = new PageId();
		PageId pidd1 = new PageId(pidd.pid);
		SystemDefs.JavabaseDB.allocate_page(pidd, howmany);
		
		try{
			pinPage(pidd1, firstPage, false, false);
			return pidd1;
		}catch(Exception e){
			SystemDefs.JavabaseDB.deallocate_page(pidd1, howmany);
			return null;
		}
	}
	
	public void freePage(PageId pgid) throws PageUnpinnedExcpetion, HashEntryNotFoundException, PagePinnedException, InvalidRunSizeException, InvalidPageNumberException, FileIOException, DiskMgrException, IOException{
		if(pg_frm.containsKey(pgid.pid)){
			int frameNum = pg_frm.get(pgid.pid);
			if(bufDescr[frameNum].pin_Count==1){
				unpinPage(pgid, bufDescr[frameNum].dirtybit, false);
				SystemDefs.JavabaseDB.deallocate_page(pgid);
			}else if(bufDescr[frameNum].pin_Count==0){
				SystemDefs.JavabaseDB.deallocate_page(pgid);
			}else{
				throw new PagePinnedException(null, "");
			}
		}else{
			SystemDefs.JavabaseDB.deallocate_page(pgid);
		}
	}
	
	public void flushPage(PageId pgid) throws InvalidPageNumberException, FileIOException, IOException{
		int frameNum = pg_frm.get(pgid.pid);
		Page tmp = new Page(bufpool[frameNum]);
		SystemDefs.JavabaseDB.write_page(pgid, tmp);
		bufDescr[frameNum].dirtybit = false;
	}

	public int getNumUnpinnedBuffers(){
		return queue.size()+numberOfFrames-numberOfPages;
	}
	
	public void flushAllPages() throws InvalidPageNumberException, FileIOException, IOException{
		for(int i=0;i<numberOfPages;i++){
			PageId pgid = new PageId(bufDescr[i].pageid);
			flushPage(pgid);
		}
	}
	
	//for debugging purposes 
	public int getNumberOfPages(){
		return numberOfPages;
	}
	public int getNumberOfFrames(){
		return numberOfFrames;
	}
	public int occupied(){
		int count = 0;
		for(int i=0;i<numberOfPages;i++){
			if(bufDescr[i].pin_Count>0)count++;
		}
		return count;
	}
	public int inPQ(){
		return queue.size();
	}
	public int inFIFO(){
		return FIFO.size();
	}
	public void printFIFO(){
		for(int i=0;i<FIFO.size();i++){
			System.out.print(FIFO.peek()+" ");
			FIFO.add(FIFO.poll());
		}System.out.println();
	}
	
}

