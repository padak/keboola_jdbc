package com.keboola.jdbc.command;

import com.keboola.jdbc.KeboolaConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Dispatches SQL to registered {@link KeboolaCommandHandler}s before it reaches
 * the Query Service. If a handler matches, its ResultSet is returned directly;
 * otherwise returns null so the caller proceeds with normal execution.
 */
public class KeboolaCommandDispatcher {

    private static final Logger LOG = LoggerFactory.getLogger(KeboolaCommandDispatcher.class);

    private final List<KeboolaCommandHandler> handlers = new ArrayList<>();

    public KeboolaCommandDispatcher() {
        handlers.add(new BackendSwitchHandler());
        handlers.add(new PullCommandHandler());
        handlers.add(new PushCommandHandler());
        handlers.add(new SessionLogHandler());
        handlers.add(new KaiCommandHandler());
        handlers.add(new HelpCommandHandler());
        handlers.add(new VirtualTableHandler());
    }

    /**
     * Attempts to handle the SQL via a registered command handler.
     *
     * @param sql        the raw SQL string
     * @param connection the parent connection
     * @return a ResultSet if a handler matched, or null to proceed with normal execution
     * @throws SQLException if the matched handler fails
     */
    public ResultSet tryHandle(String sql, KeboolaConnection connection) throws SQLException {
        for (KeboolaCommandHandler handler : handlers) {
            if (handler.canHandle(sql)) {
                LOG.debug("Command intercepted by {}: {}", handler.getClass().getSimpleName(), sql);
                return handler.execute(sql, connection);
            }
        }
        return null;
    }
}
