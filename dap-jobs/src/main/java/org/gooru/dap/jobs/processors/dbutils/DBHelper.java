package org.gooru.dap.jobs.processors.dbutils;

import org.gooru.dap.jobs.processors.nucleus.dbutils.UserBean;
import org.gooru.dap.jobs.processors.nucleus.dbutils.UserDao;
import org.gooru.dap.jobs.processors.utils.ValidatorHelper;
import org.skife.jdbi.v2.DBI;

public class DBHelper {

    private static final DBI DBI_CORE = new DBI("jdbc:postgresql://localhost:5432/nucleus", "nucleus", "nucleus");

    public static UserBean getUserDetails(String id) {
        final UserDao dao = DBI_CORE.open(UserDao.class);
        UserBean user = null;
        if (ValidatorHelper.validateUuid(id)) {
            user = dao.findUserById(id);
        } else { 
            user = dao.findUserByEmailOrRef(id);
        }
        
        System.out.println(user.getEmail());
        dao.close();
        return user;
    }
}
