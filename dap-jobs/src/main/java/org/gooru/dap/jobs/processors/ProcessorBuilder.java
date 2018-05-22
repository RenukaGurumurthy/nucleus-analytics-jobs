package org.gooru.dap.jobs.processors;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum ProcessorBuilder {

    DEFAULT("default") {
        private final Logger LOGGER = LoggerFactory.getLogger(ProcessorBuilder.class);

        @Override
        public void build() {
            LOGGER.error("Invalid job operation type passed in, not able to handle");
        }
    },
    NUCLEUS_USER_COMPETENCY_STATUS("nucleus.user.competency.status") {

        @Override
        public void build() {
            new NucleusUserCompetencyStatusProcessor().process();
        }
    },
    DAP_LEARNER_PROFILE_COMPETENCY_STAUS("dap.learner.profile.competency.status") {

        @Override
        public void build() {
            new DapLearnerProfileCompetencyStatus().process();
        }
    },
    RGO_USER_COMPETENCY_MATRIX("user.competency.matrix") {

        @Override
        public void build() {
            new RgoCompetencyMatrix().process();
        }
    };

    private String name;

    ProcessorBuilder(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    private static final Map<String, ProcessorBuilder> LOOKUP = new HashMap<>();

    static {
        for (ProcessorBuilder builder : values()) {
            LOOKUP.put(builder.getName(), builder);
        }
    }

    public static ProcessorBuilder lookupBuilder(String name) {
        ProcessorBuilder builder = LOOKUP.get(name);
        if (builder == null) {
            return DEFAULT;
        }
        return builder;
    }

    public abstract void build();
}
