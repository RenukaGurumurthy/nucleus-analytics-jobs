package org.gooru.dap.jobs.components.jdbi;

import javax.sql.DataSource;

import org.gooru.dap.jobs.components.DataSourceRegistry;
import org.gooru.dap.jobs.constants.Constants;
import org.skife.jdbi.v2.DBI;

public final class DBICreator {

    private DBICreator() {
        throw new AssertionError();
    }

    public static DBI getDbiForDapDS() {
        return createDBI(DataSourceRegistry.getInstance().getDefaultDataSource());
    }

    public static DBI getDbiForNucleusDS() {
        return createDBI(DataSourceRegistry.getInstance().getDataSourceByName(Constants.CORE_DATA_SOURCE));
    }
    
    public static DBI getDbiForRgoDS() {
        return createDBI(DataSourceRegistry.getInstance().getDataSourceByName(Constants.RGO_DATA_SOURCE));
    }

    private static DBI createDBI(DataSource dataSource) {
        DBI dbi = new DBI(dataSource);
        dbi.registerArgumentFactory(new PostgresIntegerArrayArgumentFactory());
        dbi.registerArgumentFactory(new PostgresStringArrayArgumentFactory());
        dbi.registerArgumentFactory(new PostgresUUIDArrayArgumentFactory());
        return dbi;
    }

}
