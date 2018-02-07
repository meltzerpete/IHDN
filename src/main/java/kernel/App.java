/*
 * Copyright 2018 pete meltzer
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *	   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kernel;

import java.util.ResourceBundle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 */
public class App {

	protected static final Logger logger = LoggerFactory.getLogger(App.class);
	protected static final ResourceBundle messages = ResourceBundle.getBundle("messages");

	public static void main(String[] args) {
		if (args.length > 0) {
			logger.info("{} {}! /by {}", messages.getString("hello"), args[0], messages.getString("artifactId"));
		} else {
			logger.info("Hello world!");
		}
	}

}
