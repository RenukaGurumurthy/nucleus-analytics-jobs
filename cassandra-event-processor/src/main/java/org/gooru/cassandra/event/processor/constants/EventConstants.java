package org.gooru.cassandra.event.processor.constants;

import java.util.Arrays;
import java.util.List;

/**
 * @author szgooru Created On: 27-Feb-2018
 */
public final class EventConstants {

    private EventConstants() {
        throw new AssertionError();
    }
    
    public static final String EVENT_ID = "eventId";
    public static final String EVENT_NAME = "eventName";
    public static final String EVENT_END_TIME = "endTime";
    public static final String PAYLOAD_OBJECT = "payLoadObject";
    public static final String DATA = "data";
    
    public static final String TENANT_ID = "tenant_id";
    public static final String PARTNER_ID = "partner_id";
    public static final String TENANT_ROOT = "tenant_root";
    public static final String USER_CATEGORY = "user_category";
    public static final String USER_ID = "id";
    public static final String LOGIN_TYPE = "login_type";
    
    public static final List<String> EVENTS = Arrays.asList("event.user.signin", "event.user.signout");
    
}
