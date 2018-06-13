
/**
 * Repackaging of elastic search REST Api. This is necessary since the code is not extensible. <br>
 * Changes: <br>
 * <ul>
 * <li>Modified {@link com.nc.es.rest.client.RestClient.SyncResponseListener} in order to
 * <b>not</b> throw Exceptions when response code is a 404 which might happen a lot when invoking
 * {@link com.nc.es.api.IElasticSearchOps#lookup(String, Class, String, String...)}. Let
 * the client handle it (e.g. by returning null) to the caller.</li>
 * <li>Tweaked some logging</br>
 * <li>Optimized {@link com.nc.es.rest.client.RestClient#nextHost()} method</li>
 * </ul>
 */
package com.nc.es.rest;