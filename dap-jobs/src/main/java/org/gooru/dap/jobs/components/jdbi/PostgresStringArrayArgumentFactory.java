package org.gooru.dap.jobs.components.jdbi;

import java.sql.Array;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.Argument;
import org.skife.jdbi.v2.tweak.ArgumentFactory;

public class PostgresStringArrayArgumentFactory implements ArgumentFactory<PGArray<String>> {
    @SuppressWarnings("unchecked")
    public boolean accepts(Class<?> expectedType, Object value, StatementContext ctx) {
        return value instanceof PGArray && ((PGArray) value).getType().isAssignableFrom(String.class);
    }

    @Override
    public Argument build(Class<?> expectedType, final PGArray<String> value, StatementContext ctx) {
        return (position, statement, ctx1) -> {
            Array ary = ctx1.getConnection().createArrayOf("text", value.getElements());
            statement.setArray(position, ary);
        };
    }
}
