package org.gooru.dap.jobs.processors.nucleus.dbutils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class UserCompetencyStatusBean {

    private static Map<String, Integer> COMPLETION_STATIC_LOOKUP = new HashMap<>(3);
    static {
        COMPLETION_STATIC_LOOKUP.put("in_progress", 1);
        COMPLETION_STATIC_LOOKUP.put("not_started", 0);
        COMPLETION_STATIC_LOOKUP.put("completed", 4);
        Collections.unmodifiableMap(COMPLETION_STATIC_LOOKUP);
    }
    private UUID userId;
    private String competencyId;
    private String completionStatus;
    private int status;

    public String getCompetencyId() {
        return competencyId;
    }

    public void setCompetencyId(String competencyId) {
        this.competencyId = competencyId;
    }

    public String getCompletionStatus() {
        return completionStatus;
    }

    public void setCompletionStatus(String completionStatus) {
        setStatus(COMPLETION_STATIC_LOOKUP.get(completionStatus));
        this.completionStatus = completionStatus;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public UUID getUserId() {
        return userId;
    }

    public void setUserId(UUID userId) {
        this.userId = userId;
    }

}
