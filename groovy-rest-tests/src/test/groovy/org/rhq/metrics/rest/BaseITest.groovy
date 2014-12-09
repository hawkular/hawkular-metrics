package org.rhq.metrics.rest

import org.junit.Test

import java.text.DateFormat
import java.text.SimpleDateFormat

import static junit.framework.TestCase.assertEquals
import static org.joda.time.DateTime.now
import static org.joda.time.Seconds.seconds
import static org.junit.Assert.assertTrue

class BaseITest extends RESTTest {

  @Test
  void pingTest() {
    def response = rhqm.post(path: 'ping')
    assertEquals(200, response.status)

    DateFormat df = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);
    Date date = df.parse(response.data.value);
    Date now = new Date();

    long timeDifference = now.time - date.time;

    // This is a bit problematic and depends on the system where the test is executing
    assertTrue("Difference is " + timeDifference, timeDifference < seconds(60).toStandardDuration().millis);
  }

  @Test
  void addAndGetValue() {
    def end = now().millis
    def start = end - 100
    def response = rhqm.post(path: "metrics/foo", body: [id: 'foo', timestamp: start + 10, value: 42])
    assertEquals(200, response.status)

    response = rhqm.get(path: 'metrics/foo', query: [start: start, end: end])
    assertEquals(200, response.status)
    assertEquals(start + 10, response.data[0].timestamp)
  }
 
}
