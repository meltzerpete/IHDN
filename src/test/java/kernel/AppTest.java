package kernel;

import java.io.PrintStream;
import java.util.ResourceBundle;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * Unit test for simple App.
 */
@RunWith(MockitoJUnitRunner.class)
public class AppTest {

	protected static final ResourceBundle testMessages = ResourceBundle.getBundle("testMessages");

	@Captor
	private ArgumentCaptor<String> captorLoggingMessage;

	PrintStream errorStream;
	PrintStream loggingStream;

	@Test
	public void testApp() {
		App.main(new String[] {});

		verify(loggingStream, atLeastOnce()).println(captorLoggingMessage.capture());
		final String loggingMessage = captorLoggingMessage.getValue();
		assertEquals("[main] INFO App - Hello world!", loggingMessage);
	}

	@Test
	public void testAppWithName() {
		App.main(new String[] {"John"});

		verify(loggingStream, atLeastOnce()).println(captorLoggingMessage.capture());
		final String loggingMessage = captorLoggingMessage.getValue();
		assertEquals("[main] INFO App - Hello John! /by " + testMessages.getString("artifactId"), loggingMessage);
	}

	@Before
	public void setUp() {
		errorStream = System.err;
		loggingStream = spy(errorStream);
		System.setErr(loggingStream);
	}

	@After
	public void tearDown() {
		System.setErr(errorStream);
	}

}
