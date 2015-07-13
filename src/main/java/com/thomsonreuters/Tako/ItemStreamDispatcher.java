/* final item stream serialized message bubble up device.
 */

package com.thomsonreuters.Tako;

public interface ItemStreamDispatcher {
	public void dispatch (ItemStream stream, String response);
}

/* eof */
