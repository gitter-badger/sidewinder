/**
 * Copyright 2016 Ambud Sharma
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 		http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.srotya.sidewinder.core;

import io.dropwizard.Application;
import io.dropwizard.setup.Environment;

/**
 * @author ambudsharma
 *
 */
public class SidewinderServer extends Application<SidewinderConfig>{

	@Override
	public void run(SidewinderConfig config, Environment env) throws Exception {
	}
	
	/**
	 * Main method to launch dropwizard app
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		new SidewinderServer().run(args);
	}

}
