package org.gooru.dap.jobs.processors.dbutils;

import java.util.Iterator;

import org.skife.jdbi.v2.sqlobject.BindBean;
import org.skife.jdbi.v2.sqlobject.SqlBatch;
import org.skife.jdbi.v2.sqlobject.customizers.BatchChunkSize;

public interface LearnerProfileCompetencyStatusDao {

    @SqlBatch("insert into learner_profile_competency_status (user_id, tx_subject_code, gut_code, status) values (:userId, :txSubjectCode, :gutCode, :status) "
        + "ON CONFLICT (user_id, gut_code) DO NOTHING;")
    @BatchChunkSize(1000)
    void insertAll(@BindBean Iterator<LearnerProfileCompetencyStatusBean> learnerProfileCompetencyStatusBean);

}
