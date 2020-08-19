package io.piveau.hub.util.logger;

import io.piveau.hub.dataobjects.DatasetHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MarkerIgnoringBase;

import java.util.Formatter;


/*
 *
 * Logger Class in order to enhance the log entries created.
 *
 * */
public class PiveauLogger extends MarkerIgnoringBase {


    static String baseUri = "https://io.piveau/";

    Logger log;
    String component;
    String formatString = "[%s (piveau-hub)] [%s (%s)] [%s] ";
    private String datasetId = "";
    private String catalogueId = "";

    public static void setBaseUri(String uri) {
        baseUri = uri;
    }


    PiveauLogger(Class clazz) {
        this.component = clazz.getSimpleName();
        this.log = LoggerFactory.getLogger("piveau-hub." + component);

    }

    PiveauLogger(String datasetId, String catalogueId, Class clazz) {
        this.component = clazz.getSimpleName();
        this.log = LoggerFactory.getLogger("piveau-hub." + component);
        this.catalogueId = catalogueId;
        this.datasetId = datasetId;

    }

    PiveauLogger(DatasetHelper helper, Class clazz) {
        this.component = clazz.getSimpleName();
        this.log = LoggerFactory.getLogger("piveau-hub." + component);
        this.datasetId = helper.id();
        this.catalogueId = helper.catalogueId();
    }

    protected String getPrefixMessage() {
        StringBuilder sb = new StringBuilder();
        Formatter formatter = new Formatter(sb);
        formatter.format(formatString, component, datasetId, catalogueId, baseUri);
        return sb.toString();
    }


    @Override
    public boolean isTraceEnabled() {
        return log.isTraceEnabled();
    }

    @Override
    public void trace(String msg) {
        log.trace(getPrefixMessage() + msg);
    }

    @Override
    public void trace(String format, Object arg) {
        log.trace(getPrefixMessage() + format, arg);
    }

    @Override
    public void trace(String format, Object arg1, Object arg2) {
        log.trace(getPrefixMessage() + format, arg1, arg2);
    }

    @Override
    public void trace(String format, Object... arguments) {
        log.trace(getPrefixMessage() + format, arguments);
    }

    @Override
    public void trace(String msg, Throwable t) {
        log.trace(getPrefixMessage() + msg, t);
    }

    @Override
    public boolean isDebugEnabled() {
        return log.isDebugEnabled();
    }

    @Override
    public void debug(String msg) {
        log.debug(getPrefixMessage() + msg);
    }

    @Override
    public void debug(String format, Object arg) {
        log.debug(getPrefixMessage() + format, arg);
    }

    @Override
    public void debug(String format, Object arg1, Object arg2) {
        log.debug(getPrefixMessage() + format, arg1, arg2);
    }

    @Override
    public void debug(String format, Object... arguments) {
        log.debug(getPrefixMessage() + format);
    }

    @Override
    public void debug(String msg, Throwable t) {
        log.debug(getPrefixMessage() + msg, t);
    }

    @Override
    public boolean isInfoEnabled() {
        return log.isInfoEnabled();
    }

    @Override
    public void info(String msg) {
        log.info(getPrefixMessage() + msg);
    }

    @Override
    public void info(String format, Object arg) {
        log.info(getPrefixMessage() + format, arg);
    }

    @Override
    public void info(String format, Object arg1, Object arg2) {
        log.info(getPrefixMessage() + format, arg1, arg2);
    }

    @Override
    public void info(String format, Object... arguments) {
        log.info(getPrefixMessage() + format, arguments);
    }

    @Override
    public void info(String msg, Throwable t) {
        log.info(getPrefixMessage() + msg, t);
    }

    @Override
    public boolean isWarnEnabled() {
        return log.isWarnEnabled();
    }

    @Override
    public void warn(String msg) {
        log.warn(getPrefixMessage() + msg);
    }

    @Override
    public void warn(String format, Object arg) {
        log.warn(getPrefixMessage() + format, arg);
    }

    @Override
    public void warn(String format, Object... arguments) {
        log.warn(getPrefixMessage() + format, arguments);
    }

    @Override
    public void warn(String format, Object arg1, Object arg2) {
        log.warn(getPrefixMessage() + format, arg1, arg2);
    }

    @Override
    public void warn(String msg, Throwable t) {
        log.warn(getPrefixMessage() + msg, t);
    }

    @Override
    public boolean isErrorEnabled() {
        return log.isErrorEnabled();
    }

    @Override
    public void error(String msg) {
        log.error(getPrefixMessage() + msg);
    }

    @Override
    public void error(String format, Object arg) {
        log.error(getPrefixMessage() + format, arg);
    }

    @Override
    public void error(String format, Object arg1, Object arg2) {
        log.error(getPrefixMessage() + format, arg1, arg2);
    }

    @Override
    public void error(String format, Object... arguments) {
        log.error(getPrefixMessage() + format, arguments);
    }

    @Override
    public void error(String msg, Throwable t) {
        log.error(getPrefixMessage() + msg, t);
    }

}

