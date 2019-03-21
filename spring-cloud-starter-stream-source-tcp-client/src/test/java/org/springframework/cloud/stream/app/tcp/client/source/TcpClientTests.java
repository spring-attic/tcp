/*
 * Copyright 2016-2017 the original author or authors.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.OutputStream;
import java.net.ServerSocket;
import java.util.concurrent.TimeUnit;

import javax.net.ServerSocketFactory;

import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.context.annotation.Import;
import org.springframework.integration.ip.tcp.connection.AbstractConnectionFactory;
import org.springframework.integration.ip.tcp.connection.TcpNioClientConnectionFactory;
import org.springframework.integration.ip.tcp.serializer.ByteArrayLfSerializer;
import org.springframework.integration.test.util.TestUtils;
import org.springframework.messaging.Message;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * Tests for TcpClient source.
 *
 * @author Ilayaperumal Gopinathan
 * @author Gary Russell
 */
@RunWith(SpringRunner.class)
@DirtiesContext
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
	properties = { "tcp.host = localhost", "tcp.port = ${tcp.client.test.port}" })
public abstract class TcpClientTests {

	@Autowired
	protected Source source;

	@Autowired
	protected MessageCollector messageCollector;

	@Autowired
	protected AbstractConnectionFactory connectionFactory;

	private static ServerSocket serverSocket;

	@BeforeClass
	public static void startup() throws Exception {
		serverSocket = ServerSocketFactory.getDefault().createServerSocket(0);
		System.setProperty("tcp.client.test.port", Integer.toString(serverSocket.getLocalPort()));
	}

	@AfterClass
	public static void shutDown() throws Exception {
		serverSocket.close();
	}

	@TestPropertySource(properties = { "tcp.host = foo", "tcp.decoder=LF", "tcp.bufferSize=1234", "tcp.nio = true",
			"tcp.reverseLookup = true", "tcp.useDirectBuffers = true", "tcp.socketTimeout = 123", "tcp.charset = bar" })
	public static class PropertiesPopulatedTests extends TcpClientTests {

		@Test
		public void test() throws Exception {
			assertThat(this.connectionFactory, Matchers.instanceOf(TcpNioClientConnectionFactory.class));
			assertEquals("foo", this.connectionFactory.getHost());
			assertTrue(this.connectionFactory.getDeserializer() instanceof ByteArrayLfSerializer);
			assertEquals(1234, ((ByteArrayLfSerializer) this.connectionFactory.getDeserializer()).getMaxMessageSize());
			assertTrue(TestUtils.getPropertyValue(this.connectionFactory, "lookupHost", Boolean.class));
			assertTrue(TestUtils.getPropertyValue(this.connectionFactory, "usingDirectBuffers", Boolean.class));
			assertEquals(123, TestUtils.getPropertyValue(this.connectionFactory, "soTimeout"));
			assertFalse(this.connectionFactory.isSingleUse());
			assertEquals("bar", TestUtils.getPropertyValue(this.connectionFactory, "mapper.charset"));
		}

	}


	public static class TestTcpClient extends TcpClientTests {

		@Test
		public void testOutput() throws Exception {
			OutputStream outputStream = serverSocket.accept().getOutputStream();
			outputStream.write("Test1\r\n".getBytes());
			Message<?> message = messageCollector.forChannel(source.output()).poll(5000, TimeUnit.MILLISECONDS);
			assertNotNull(message);
			assertEquals("Test1", new String((byte[]) message.getPayload()));
			outputStream.write("Test2\r\n".getBytes());
			message =  messageCollector.forChannel(source.output()).poll(5000, TimeUnit.MILLISECONDS);
			assertNotNull(message);
			assertEquals("Test2", new String((byte[]) message.getPayload()));
		}

	}

	@SpringBootApplication
	@Import(TcpClientSourceConfiguration.class)
	public static class TcpClientApplication {

	}

}
