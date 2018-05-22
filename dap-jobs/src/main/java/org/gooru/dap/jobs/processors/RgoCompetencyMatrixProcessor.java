package org.gooru.dap.jobs.processors;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.gooru.dap.jobs.components.jdbi.DBICreator;
import org.gooru.dap.jobs.processors.nucleus.dbutils.NucleusDBHelper;
import org.gooru.dap.jobs.processors.nucleus.dbutils.UserBean;
import org.gooru.dap.jobs.processors.rgo.dbutils.UserCompetencyMatrixBean;
import org.gooru.dap.jobs.processors.rgo.dbutils.UserCompetencyMatrixDao;
import org.gooru.dap.jobs.processors.utils.UtilHelper;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RgoCompetencyMatrix implements Processor {

    private static final Logger LOGGER = LoggerFactory.getLogger(RgoCompetencyMatrix.class);

    private static final DBI DBI_RGO = DBICreator.getDbiForRgoDS();

    @Override
    public void process() {
        String filePath = System.getProperty("data.ingestion.filepath");
        if (filePath != null) {
            LOGGER.info("RGO -  User Competency Matrix Data ingestion job started...");
            File folder = new File(filePath);
            for (final File file : folder.listFiles()) {
                readExcelSheetAndUpdate(file);
            }
            LOGGER.info("RGO - User Competency Matrix Data ingestion job completed...");
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
            Set<UserCompetencyMatrixBean> userCompetencyMatrixBeans = new HashSet<>();
            Sheet sheet = workbook.getSheet("Mastery with GUT IDs");
            for (Row row : sheet) {
                if (row.getRowNum() > 0) {
                    final String gutId = UtilHelper.getCellValue(row.getCell(0));
                    final String status = UtilHelper.getCellValue(row.getCell(1));
                    if (gutId != null && !gutId.trim().equalsIgnoreCase("#N/A") && status != null
                        && status.equalsIgnoreCase("completed")) {
                        UserCompetencyMatrixBean userCompetencyMatrixBean =
                            new UserCompetencyMatrixBean();
                        userCompetencyMatrixBean.setUserId(userBean.getUserId());
                        userCompetencyMatrixBean.setTxCompCode(gutId);
                        userCompetencyMatrixBean.setCompletionStatus(status);
                        userCompetencyMatrixBean.setTxSubjectCode(UtilHelper.getSubjectIdFromGutId(gutId));
                        userCompetencyMatrixBeans.add(userCompetencyMatrixBean);
                    }

                }
            }

            workbook.close();

            if (userCompetencyMatrixBeans.size() > 0) {
                Handle handle = DBI_RGO.open();
                UserCompetencyMatrixDao userCompetencyMatrixDao =
                    handle.attach(UserCompetencyMatrixDao.class);
                userCompetencyMatrixDao.insertAll(userCompetencyMatrixBeans.iterator());
                handle.commit();
                handle.close();
            }

        } catch (Exception e) {
            LOGGER.error("Failed to parse and read the information from Excel sheet and write to database {}", e);
        }
    }

}
