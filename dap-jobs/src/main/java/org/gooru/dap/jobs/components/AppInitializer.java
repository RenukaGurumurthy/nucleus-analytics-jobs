package org.gooru.dap.jobs.components;

import java.util.ArrayList;
import java.util.List;


public final class AppInitializer {

    private AppInitializer() {
        throw new AssertionError();
    }

    private static final List<InitializationAwareComponent> initializers = new ArrayList<>();

    static {
        initializers.add(DataSourceRegistry.getInstance());
    }

    public static void initializeApp() {
        for (InitializationAwareComponent initializer : initializers) {
            initializer.initializeComponent();
        }

    }
}
