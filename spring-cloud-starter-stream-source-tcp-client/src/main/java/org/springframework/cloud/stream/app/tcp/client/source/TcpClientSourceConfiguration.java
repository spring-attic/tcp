/*
 * Copyright 2016-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.app.tcp.client.source;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.app.tcp.EncoderDecoderFactoryBean;
import org.springframework.cloud.stream.app.tcp.TcpConnectionFactoryProperties;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.ip.config.TcpConnectionFactoryFactoryBean;
import org.springframework.integration.ip.tcp.TcpReceivingChannelAdapter;
import org.springframework.integration.ip.tcp.connection.AbstractConnectionFactory;
import org.springframework.integration.ip.tcp.connection.TcpMessageMapper;
import org.springframework.integration.ip.tcp.serializer.AbstractByteArraySerializer;

/**
 * A source application that acts as a TCP client.
 *
 * @author Ilayaperumal Gopinathan
 * @author Gary Russell
 * @author Christian Tzolov
 */
@EnableBinding(Source.class)
@EnableConfigurationProperties({ TcpClientSourceProperties.class, TcpConnectionFactoryProperties.class })
public class TcpClientSourceConfiguration {

	@Autowired
	private TcpClientSourceProperties properties;

	@Autowired
	private TcpConnectionFactoryProperties tcpConnectionProperties;

	@Bean
	public TcpReceivingChannelAdapter adapter(
			@Qualifier("tcpClientConnectionFactory") AbstractConnectionFactory connectionFactory) {
		TcpReceivingChannelAdapter adapter = new TcpReceivingChannelAdapter();
		adapter.setConnectionFactory(connectionFactory);
		adapter.setClientMode(true);
		adapter.setRetryInterval(this.properties.getRetryInterval());
		adapter.setOutputChannelName(Source.OUTPUT);
		return adapter;
	}

	@Bean
	public TcpConnectionFactoryFactoryBean tcpClientConnectionFactory(
			@Qualifier("tcpClientMapper") TcpMessageMapper mapper,
			@Qualifier("tcpClientDecoder") AbstractByteArraySerializer decoder) throws Exception {
		TcpConnectionFactoryFactoryBean factoryBean = new TcpConnectionFactoryFactoryBean();
		factoryBean.setType("client");
		factoryBean.setHost(this.properties.getHost());
		factoryBean.setPort(this.tcpConnectionProperties.getPort());
		factoryBean.setUsingNio(this.tcpConnectionProperties.isNio());
		factoryBean.setUsingDirectBuffers(this.tcpConnectionProperties.isUseDirectBuffers());
		factoryBean.setLookupHost(this.tcpConnectionProperties.isReverseLookup());
		factoryBean.setDeserializer(decoder);
		factoryBean.setSoTimeout(this.tcpConnectionProperties.getSocketTimeout());
		factoryBean.setMapper(mapper);
		return factoryBean;
	}

	@Bean
	public TcpMessageMapper tcpClientMapper() {
		TcpMessageMapper mapper = new TcpMessageMapper();
		mapper.setCharset(this.properties.getCharset());
		return mapper;
	}

	@Bean
	public EncoderDecoderFactoryBean tcpClientDecoder() {
		EncoderDecoderFactoryBean factoryBean = new EncoderDecoderFactoryBean(this.properties.getDecoder());
		factoryBean.setMaxMessageSize(this.properties.getBufferSize());
		return factoryBean;
	}

}
