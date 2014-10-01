package bufmgr;
import diskmgr.*;
public class BufPool {
	Descriptor [] bufdescr; 
	Page [] bufpool ;
	public BufPool(int numbuf){
		bufpool = new Page[numbuf];
		bufdescr = new Descriptor [numbuf];
	}
}
