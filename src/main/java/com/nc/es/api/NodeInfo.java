package com.nc.es.api;

public class NodeInfo implements IElasticSearchObject {

	static class VersionInfo {
		String number;
		String build_hash;
		String build_date;
		String build_snapshot;
		String lucene_version;

	}

	String name;
	String cluster_name;
	String cluster_uuid;
	VersionInfo version;
	String tagline;

	public String toString() {
		return asPrettyJson();
	}

	public boolean supportsTypes() {
		return version != null && version.number != null && version.number.startsWith("5");
	}

}
