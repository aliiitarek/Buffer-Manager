package bufmgr;
import global.PageId;
import diskmgr.*;
public class Descriptor {
	protected int pin_Count,user_counter,pageid;
	protected boolean dirtybit,loved;
	
	
	public Descriptor(){
		pageid = 0;
		pin_Count = 1;
		dirtybit = false;
		user_counter = 1;
		loved = false;
	}
	public Descriptor(int pn,int pc,boolean db,int uc,boolean lv){
		pageid = pn;
		pin_Count = pc;
		dirtybit = db;
		user_counter = uc;
		loved = lv;
	}
}
