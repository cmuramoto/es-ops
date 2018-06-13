package com.nc.es.rest.sniff;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.http.HttpHost;

import com.nc.es.rest.client.RestClient;

/**
 * {@link org.elasticsearch.client.RestClient.FailureListener} implementation that allows to perform
 * sniffing on failure. Gets notified whenever a failure happens and uses a {@link Sniffer} instance
 * to manually reload hosts and sets them back to the {@link RestClient}. The {@link Sniffer}
 * instance needs to be lazily set through {@link #setSniffer(Sniffer)}.
 */
public class SniffOnFailureListener extends RestClient.FailureListener {

	private volatile Sniffer sniffer;
	private final AtomicBoolean set;

	public SniffOnFailureListener() {
		this.set = new AtomicBoolean(false);
	}

	/**
	 * Sets the {@link Sniffer} instance used to perform sniffing
	 * 
	 * @throws IllegalStateException
	 *             if the sniffer was already set, as it can only be set once
	 */
	public void setSniffer(Sniffer sniffer) {
		Objects.requireNonNull(sniffer, "sniffer must not be null");
		if (set.compareAndSet(false, true)) {
			this.sniffer = sniffer;
		} else {
			throw new IllegalStateException("sniffer can only be set once");
		}
	}

	@Override
	public void onFailure(HttpHost host) {
		if (sniffer == null) {
			throw new IllegalStateException("sniffer was not set, unable to sniff on failure");
		}
		// re-sniff immediately but take out the node that failed
		sniffer.sniffOnFailure(host);
	}
}
