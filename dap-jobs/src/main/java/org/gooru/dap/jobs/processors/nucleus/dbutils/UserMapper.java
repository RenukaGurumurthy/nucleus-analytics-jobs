package org.gooru.dap.jobs.processors.nucleus.dbutils;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

public class UserMapper implements ResultSetMapper<UserBean> {

    private final static String ID = "id";
    private final static String EMAIL = "email";
    private final static String USER_NAME = "username";
    private final static String REFERENCE_ID = "reference_id";

    @Override
    public UserBean map(int index, ResultSet r, StatementContext ctx) throws SQLException {
        UserBean userBean = new UserBean();
        userBean.setEmail(r.getString(EMAIL));
        userBean.setUsername(r.getString(USER_NAME));
        userBean.setReferenceId(r.getString(REFERENCE_ID));
        userBean.setUserId(r.getString(ID));
        return userBean;
    }

}
