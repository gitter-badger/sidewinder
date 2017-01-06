package com.srotya.sidewinder.core.api;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

@Path("/hc")
public class HealthCheck {
	
	@GET
	public String hc() {
		return "healthy";
	}

}
