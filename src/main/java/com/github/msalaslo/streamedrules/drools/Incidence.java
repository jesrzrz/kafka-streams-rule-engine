package com.github.msalaslo.streamedrules.drools;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * A simple object to store a message for processing by Drools.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Incidence {
	private String id;
	private String type;
	private String param1;
	private String param2;
	private int param3;
	private boolean param4;
	private String callType;
	private String observations;
	private String problem;
}