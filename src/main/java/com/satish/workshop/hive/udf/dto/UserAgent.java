package com.satish.workshop.hive.udf.dto;

public class UserAgent {
	private String type = null;
	private String agent = null;
	private String browserVersion = null;
	
	public UserAgent (String type, String agent, String browserVersion) {
		this.type = type;
		this.agent = agent;
		this.browserVersion = browserVersion;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getAgent() {
		return agent;
	}

	public void setAgent(String agent) {
		this.agent = agent;
	}

	public String getBrowserVersion() {
		return browserVersion;
	}

	public void setBrowserVersion(String browserVersion) {
		this.browserVersion = browserVersion;
	}
	
	
}
