package com.azure.cosmos.mongostream;

public class LeaseDocument {

	public String id;
	public String resumeToken;
	public long ownerHealthCheckRefrehTime;
	public String hostName;

}
