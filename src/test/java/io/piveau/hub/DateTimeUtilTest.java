package io.piveau.hub;

import io.piveau.hub.util.rdf.DateTimeUtil;
import io.vertx.junit5.VertxExtension;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Testing date time utils")
class DateTimeUtilTest {

    @Test
    @DisplayName("Parsing several date and dateTime strings")
    void parseTest() {
        assertNotNull(new DateTimeUtil().now());
        assertNotNull(new DateTimeUtil().parse("2019-07-01T12:00:20Z"));
        assertNotNull(new DateTimeUtil().parse("2019-07-01T12:00:20.00Z"));
        assertNotNull(new DateTimeUtil().parse("2019-07-01T12:00:20.00+01:00"));
        assertNotNull(new DateTimeUtil().parse("2019-07-01T12:00:20.00+02"));
        assertNotNull(new DateTimeUtil().parse("2019-07-01T12:00:20"));
        assertNotNull(new DateTimeUtil().parse("2019-07-01T12:00:20.00000"));
        assertNotNull(new DateTimeUtil().parse("2019-07-01"));
        assertNotNull(new DateTimeUtil().parse("2019/06/01"));

        assertNull(new DateTimeUtil().parse("blabla"));
    }
}
