package org.gooru.dap.jobs.processors.rgo.dbutils;

import java.util.Iterator;

import org.skife.jdbi.v2.sqlobject.BindBean;
import org.skife.jdbi.v2.sqlobject.SqlBatch;
import org.skife.jdbi.v2.sqlobject.customizers.BatchChunkSize;

public interface UserCompetencyMatrixDao {

    @SqlBatch("insert into user_competency_matrix (user_id, tx_subject_code, tx_comp_code, status) values (:userId, :txSubjectCode, :txCompCode, :status) "
        + "ON CONFLICT (user_id, tx_comp_code) DO NOTHING;")
    @BatchChunkSize(1000)
    void insertAll(@BindBean Iterator<UserCompetencyMatrixBean> userCompetencyMatrixBean);

}
