package org.gooru.dap.jobs.processors.nucleus.dbutils;

import java.util.Iterator;

import org.skife.jdbi.v2.sqlobject.BindBean;
import org.skife.jdbi.v2.sqlobject.SqlBatch;
import org.skife.jdbi.v2.sqlobject.customizers.BatchChunkSize;

public interface UserCompetencyStatusDao {

    @SqlBatch("insert into user_competency_status (user_id, comp_mcomp_id, completion_status) values (:userId, :competencyId, :status) "
        + "ON CONFLICT (user_id, comp_mcomp_id) DO NOTHING;")
    @BatchChunkSize(1000)
    void insertAll(@BindBean Iterator<UserCompetencyStatusBean> userCompetencyStatusBean);

    
    /**
     * close with no args is used to close the connection
     */
    void close();
}
