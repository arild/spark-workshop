package workshop.util

import org.scalatest.Matchers

class IpLookupTest extends SparkTestUtils with Matchers {
  test("test") {
    IpLookup.getCountryForIp("1.2.3.4").get should be("United States")

  }
}
