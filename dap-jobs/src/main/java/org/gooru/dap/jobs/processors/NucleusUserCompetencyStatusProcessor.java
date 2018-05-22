package org.gooru.dap.jobs.processors;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.gooru.dap.jobs.components.jdbi.DBICreator;
import org.gooru.dap.jobs.processors.nucleus.dbutils.NucleusDBHelper;
import org.gooru.dap.jobs.processors.nucleus.dbutils.UserBean;
import org.gooru.dap.jobs.processors.nucleus.dbutils.UserCompetencyStatusBean;
import org.gooru.dap.jobs.processors.nucleus.dbutils.UserCompetencyStatusDao;
import org.gooru.dap.jobs.processors.utils.UtilHelper;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class NucleusUserCompetencyStatusProcessor implements Processor {

    private static final Logger LOGGER = LoggerFactory.getLogger(NucleusUserCompetencyStatusProcessor.class);

    private static final DBI DBI_NUCLEUS = DBICreator.getDbiForNucleusDS();

    @Override
    public void process() {
        String filePath = System.getProperty("data.ingestion.filepath");
        if (filePath != null) {
            LOGGER.info("Nucleus - User Competency Status Data ingestion job started...");
            File folder = new File(filePath);
            for (final File file : folder.listFiles()) {
                readExcelSheetAndUpdate(file);
            }
            LOGGER.info("Nucleus - User Competency Status Data ingestion job completed...");
        } else {
            LOGGER.error("Data ingestion filepath should not be null");

        }
    }

    private void readExcelSheetAndUpdate(File file) {
        try {
            final String extractUserInfoFromFilename =
                file.getName().replaceFirst("LUSD_Skyline_", "").replaceFirst(".xlsx", "");
            final UserBean userBean = NucleusDBHelper.getUserDetails(extractUserInfoFromFilename);
            if (userBean == null) {
                LOGGER.error("User details not found!!, tried to extract from filename  : {}  extractedInfo : ",
                    file.getName(), extractUserInfoFromFilename);
                throw new IOException();
            }
            final Workbook workbook = WorkbookFactory.create(file);
            Set<UserCompetencyStatusBean> userCompetencyStatusBeans = new HashSet<>();
            Sheet sheet = workbook.getSheet("Mastery with GUT IDs");
            for (Row row : sheet) {
                if (row.getRowNum() > 0) {
                    final String gutId = UtilHelper.getCellValue(row.getCell(0));
                    final String status = UtilHelper.getCellValue(row.getCell(1));
                    if (gutId != null && !gutId.trim().equalsIgnoreCase("#N/A") && status != null
                        && status.equalsIgnoreCase("completed")) {
                        UserCompetencyStatusBean userCompetencyStatusBean = new UserCompetencyStatusBean();
                        userCompetencyStatusBean.setUserId(UUID.fromString(userBean.getUserId()));
                        userCompetencyStatusBean.setCompetencyId(gutId);
                        userCompetencyStatusBean.setCompletionStatus(status);
                        userCompetencyStatusBeans.add(userCompetencyStatusBean);
                    }

                }
            }

            workbook.close();

            if (userCompetencyStatusBeans.size() > 0) {
                Handle handle = DBI_NUCLEUS.open();
                UserCompetencyStatusDao userCompetencyStatusDao = handle.attach(UserCompetencyStatusDao.class);
                userCompetencyStatusDao.insertAll(userCompetencyStatusBeans.iterator());
                handle.commit();
                handle.close();
            }

        } catch (Exception e) {
            LOGGER.error("Failed to parse and read the information from Excel sheet and write to database {}", e);
        }
    }

}
