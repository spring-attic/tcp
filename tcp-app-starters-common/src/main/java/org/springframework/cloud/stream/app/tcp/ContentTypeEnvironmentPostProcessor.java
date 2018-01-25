/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.app.tcp;

import java.util.Properties;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.PropertiesPropertySource;

/**
 * An {@EnvironmentPostProcessor} to set the {@code spring.cloud.stream.bindings.output.contentType}
 * property to {@code application/octet-stream} if it has not been set already.
 *
 * @author Chris Schaefer
 */
public class ContentTypeEnvironmentPostProcessor implements EnvironmentPostProcessor {
	protected static final String PROPERTY_SOURCE_KEY_NAME = "tcp.contentType";
	protected static final String CONTENT_TYPE_PROPERTY_VALUE = "application/octet-stream";
	protected static final String CONTENT_TYPE_PROPERTY_KEY = "spring.cloud.stream.bindings.output.contentType";

	@Override
	public void postProcessEnvironment(ConfigurableEnvironment configurableEnvironment, SpringApplication springApplication) {
		if(!configurableEnvironment.containsProperty(CONTENT_TYPE_PROPERTY_KEY)) {
			Properties properties = new Properties();
			properties.setProperty(CONTENT_TYPE_PROPERTY_KEY, CONTENT_TYPE_PROPERTY_VALUE);

			PropertiesPropertySource propertiesPropertySource =
					new PropertiesPropertySource(PROPERTY_SOURCE_KEY_NAME, properties);
			configurableEnvironment.getPropertySources().addLast(propertiesPropertySource);
		}
	}
}