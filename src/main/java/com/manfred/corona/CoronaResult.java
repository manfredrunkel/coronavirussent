package com.manfred.corona;

public class CoronaResult {

	private Long timestamp;
	private Integer newsTotalCount;
	private Long newsTotalCoronaVirus;
	private double tone;
	
	public Long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}
	public Integer getNewsTotalCount() {
		return newsTotalCount;
	}
	public void setNewsTotalCount(Integer newsTotalCount) {
		this.newsTotalCount = newsTotalCount;
	}
	public Long getNewsTotalCoronaVirus() {
		return newsTotalCoronaVirus;
	}
	public void setNewsTotalCoronaVirus(Long newsTotalCoronaVirus) {
		this.newsTotalCoronaVirus = newsTotalCoronaVirus;
	}
	
	@Override
	public String toString() {
		return timestamp + "/" + tone ;
	}
	public double getTone() {
		return tone;
	}
	public void setTone(double tone) {
		this.tone = tone;
	}
	
	
	
}
