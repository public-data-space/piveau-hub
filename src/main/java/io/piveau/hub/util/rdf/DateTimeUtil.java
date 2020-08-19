package io.piveau.hub.util.rdf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class DateTimeUtil {

    private final Logger log = LoggerFactory.getLogger(getClass());

    public String parse(String inputDate) {
        return parseZonedDateTime(inputDate);
    }

    public String now() {
        return Instant.now().toString();
    }

    private String parseZonedDateTime(String dateTime) {
        try {
            return ZonedDateTime.parse(dateTime).toInstant().toString();
        } catch (DateTimeParseException e) {
            return parseLocalDateTime(dateTime);
        }
    }

    private String parseLocalDateTime(String dateTime) {
        try {
            return LocalDateTime.parse(dateTime).atZone(ZoneId.systemDefault()).toInstant().toString();
        } catch (DateTimeParseException e) {
            return parseLocalDate(dateTime);
        }
    }

    private String parseLocalDate(String date) {
        try {
            return LocalDate.parse(date).atStartOfDay().toInstant(ZoneOffset.UTC).toString();
        } catch (DateTimeParseException e) {
            return parseDate(date);
        }
    }

    private String parseDate(String date) {
        try {
            return LocalDate.parse(date, DateTimeFormatter.ofPattern("yyyy/MM/dd")).atStartOfDay().toInstant(ZoneOffset.UTC).toString();
        } catch (DateTimeParseException e) {
            log.debug("Parsing {}", date, e);
            return null;
        }
    }

}
