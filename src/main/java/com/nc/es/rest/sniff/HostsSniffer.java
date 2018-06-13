package com.nc.es.rest.sniff;

import java.io.IOException;
import java.util.List;

import org.apache.http.HttpHost;

/**
 * Responsible for sniffing the http hosts
 */
public interface HostsSniffer {
	/**
	 * Returns the sniffed http hosts
	 */
	List<HttpHost> sniffHosts() throws IOException;
}
