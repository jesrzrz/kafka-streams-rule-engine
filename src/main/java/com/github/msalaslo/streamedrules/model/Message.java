package com.github.msalaslo.streamedrules.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * A simple object to store a message for processing by Drools.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Message {
	private String content;
}