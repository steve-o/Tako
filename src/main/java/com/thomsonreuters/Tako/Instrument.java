/* Subscription symbol.
 */

package com.thomsonreuters.Tako;

import com.reuters.rfa.common.Handle;
import com.google.gson.GsonBuilder;

public class Instrument {
	private String service;
	private String name;
	private String[] fields;

	public Instrument (String service, String name, String[] fields) {
		this.setService (service);
		this.setName (name);
		this.setFields (fields);
	}

	public String getService() {
		return this.service;
	}

	public void setService (String service) {
		this.service = service;
	}

	public String getName() {
		return this.name;
	}

	public void setName (String name) {
		this.name = name;
	}

	public String[] getFields() {
		return this.fields;
	}

	public void setFields (String[] fields) {
		this.fields = fields;
	}

	@Override
	public String toString() {
		return new GsonBuilder()
			.disableHtmlEscaping()
			.create()
			.toJson (this);
	}
}

/* eof */
