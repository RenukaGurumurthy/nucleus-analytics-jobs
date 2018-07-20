package org.gooru.dap.jobs.processors.utils;

import org.apache.poi.ss.usermodel.Cell;

public class UtilHelper {

    public static final String getCellValue(Cell cell) {
        if (cell != null) {
            switch (cell.getCellTypeEnum()) {
            case BOOLEAN:
                return String.valueOf(cell.getBooleanCellValue());
            case NUMERIC:
                return String.valueOf(cell.getNumericCellValue());
            case STRING:
                return cell.getStringCellValue();
            case FORMULA:
                return cell.getRichStringCellValue().getString();
            default:
                return null;
            }
        }
        return null;
    }

    public static final String getSubjectIdFromGutId(String gutId) {
        String[] ids = gutId.split("-");
        if (ids.length > 1) {
            return ids[0];
        }
        return null;
    }

}
