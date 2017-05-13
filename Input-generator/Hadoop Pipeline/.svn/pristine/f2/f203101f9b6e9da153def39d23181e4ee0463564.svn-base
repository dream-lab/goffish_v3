package in.dream_lab.hadoopPipeline.cc;

/*This will store edge object with edgeId 
 * & flag to indicate it is remote or not*/

public class IEdge {
	
	public long  edgeId;
	public boolean isRemote;  

	
	public void  createEdge(long id) {
	    this.edgeId = id;
	    this.isRemote = false;
	  }
	
	public void setIsRemote(){
		this.isRemote = true;
	}
	
    public boolean checkRemote(){
    	
    	return this.isRemote;
    }
	
    public long getEdgeId(){
    	return this.edgeId;
    }
    
}
