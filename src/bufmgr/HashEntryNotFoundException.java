package bufmgr;
import chainexception.*;


public class HashEntryNotFoundException extends ChainException {

  public HashEntryNotFoundException(Exception e, String name)
  
  { 
    super(e, name); 
  }


}


