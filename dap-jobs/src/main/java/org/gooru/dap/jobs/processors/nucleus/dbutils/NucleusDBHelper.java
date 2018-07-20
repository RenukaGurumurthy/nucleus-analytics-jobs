package org.gooru.dap.jobs.processors.nucleus.dbutils;

import org.gooru.dap.jobs.components.jdbi.DBICreator;
import org.gooru.dap.jobs.processors.utils.ValidatorHelper;
import org.skife.jdbi.v2.DBI;

public class NucleusDBHelper {

    private static final DBI DBI_NUCLEUS = DBICreator.getDbiForNucleusDS();

    public static UserBean getUserDetails(String id) {
        final UserDao dao = DBI_NUCLEUS.open(UserDao.class);
        UserBean user = null;
        if (ValidatorHelper.validateUuid(id)) {
            user = dao.findUserById(id);
        } else { 
            user = dao.findUserByEmailOrRef(id);
        }        
        dao.close();
        return user;
    }

}
