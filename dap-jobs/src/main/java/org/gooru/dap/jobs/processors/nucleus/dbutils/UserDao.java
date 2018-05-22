package org.gooru.dap.jobs.processors.nucleus.dbutils;

import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterMapper;

public interface UserDao {

    @SqlQuery("select id::text, email, username, reference_id from users where (email = :id OR reference_id = :id)")
    @RegisterMapper(UserMapper.class)
    UserBean findUserByEmailOrRef(@Bind("id") String id);

    @SqlQuery("select id::text, email, username, reference_id from users where id = :id")
    @RegisterMapper(UserMapper.class)
    UserBean findUserById(@Bind("id") String id);

    /**
     * close with no args is used to close the connection
     */
    void close();
}
