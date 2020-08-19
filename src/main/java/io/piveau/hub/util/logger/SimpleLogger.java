package io.piveau.hub.util.logger;

import java.util.Formatter;

public class SimpleLogger extends PiveauLogger {
    SimpleLogger(Class clazz) {
        super(clazz);
        formatString="[%s (piveau-hub)] [- (-)] [%s] ";
    }


@Override
    protected String getPrefixMessage() {
        StringBuilder sb = new StringBuilder();
        Formatter formatter = new Formatter(sb);
        formatter.format(formatString, component, baseUri);
        return sb.toString();
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
