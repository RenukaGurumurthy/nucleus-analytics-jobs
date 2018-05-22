package org.gooru.dap.jobs.components.jdbi;

import java.util.Collection;


public class PGArray<T> {
    private final Object[] elements;
    private final Class<T> type;

    public PGArray(Class<T> type, Collection<T> elements) {
        this.elements = this.toArray(elements);
        this.type = type;
    }

    private Object[] toArray(Collection<T> elements) {
        return elements.toArray();
    }

    public static <T> PGArray<T> arrayOf(Class<T> type, Collection<T> elements) {
        return new PGArray<>(type, elements);
    }

    public Object[] getElements() {
        return this.elements;
    }

    public Class<T> getType() {
        return this.type;
    }
}

