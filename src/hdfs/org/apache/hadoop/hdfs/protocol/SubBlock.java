package org.apache.hadoop.hdfs.protocol;

public class SubBlock extends Block{
	private long sizePerSubblock;
	private long length;
	private long offSetInBlock;
	
	public SubBlock(long sizePerSub, long len, long off, long blockid, long blocksize, long gen) {
		super(blockid, blocksize, gen);
		this.sizePerSubblock = sizePerSub;
		this.length = len;
		this.offSetInBlock = off;
	}
	
	public SubBlock(long sizePerSub, long len, long off, Block block) {
		super(block);
		this.sizePerSubblock = sizePerSub;
		this.length = len;
		this.offSetInBlock = off;
	}
	
	public long getSizePerSubblock() {
		return sizePerSubblock;
	}
	
	public long getLength() {
		return length;
	}
	
	public long getOffSetInBlock() {
		return offSetInBlock;
	}
	
	public void setLength(long len) {
		this.length = len;
	}
	
	public void setOffSetInBlock(long off) {
		this.offSetInBlock = off;
	}
	
	

}
