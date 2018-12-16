package com.shufan.model;

import org.codehaus.jackson.annotate.JsonProperty;

public class StepCount {
	private int user_id;
	private int day;
	private int time_interval;
	private int step_count;

	
	public StepCount(int user_id, int day, int time_interval, int step_count) {
		this.user_id = user_id;
		this.day = day;
		this.time_interval = time_interval;
		this.step_count = step_count;
	}
	
//	@JsonProperty(value = "id")
//	public long getId() {
//		return id;
//	}
//	
//	public void setId(long id) {
//		this.id = id;
//	}
	
	@JsonProperty(value = "user_id")
	public int getUserId() {
		return user_id;
	}
	
	public void setUserId(int user_id) {
		this.user_id = user_id;
	}
	
	@JsonProperty(value = "day")
	public int getDay() {
		return day;
	}
	
	public void setDay(int day) {
		this.day = day;
	}
	
	@JsonProperty(value = "time_interval")
	public int getTimeInterval() {
		return time_interval;
	}
	
	public void setTimeInterval(int time_interval) {
		this.time_interval = time_interval;
	}
	
	@JsonProperty(value = "step_count")
	public int getStepCount() {
		return step_count;
	}
	
	public void setStepCount(int step_count) {
		this.step_count = step_count;
	}

	@Override
	public String toString() {
		return "StepCount [user_id=" + user_id
				+ ", day=" + day + ", time_interval=" + time_interval + ", step_count=" + step_count + "]";
	}
	
	
}
