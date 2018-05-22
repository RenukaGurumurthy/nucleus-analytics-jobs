package org.gooru.dap.jobs.processors.dbutils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class LearnerProfileCompetencyStatusBean {

    private static Map<String, Integer> COMPLETION_STATIC_LOOKUP = new HashMap<>(3);
    static {
        COMPLETION_STATIC_LOOKUP.put("in_progress", 1);
        COMPLETION_STATIC_LOOKUP.put("not_started", 0);
        COMPLETION_STATIC_LOOKUP.put("completed", 4);
        Collections.unmodifiableMap(COMPLETION_STATIC_LOOKUP);
    }
    private String userId;
    private String gutCode;
    private String completionStatus;
    private int status;
    private String txSubjectCode;

    public String getGutCode() {
        return gutCode;
    }

    public void setGutCode(String gutCode) {
        this.gutCode = gutCode;
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

    public String getTxSubjectCode() {
        return txSubjectCode;
    }

    public void setTxSubjectCode(String txSubjectCode) {
        this.txSubjectCode = txSubjectCode;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

}
