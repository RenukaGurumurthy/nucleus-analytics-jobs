package org.gooru.dap.jobs.processors.utils;

import java.util.UUID;

public class ValidatorHelper {

    public static boolean validateUuid(Object o) {
        try {
            UUID.fromString((String) o);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
}
