package org.gooru.cassandra.event.processor.entities;

import org.gooru.cassandra.event.processor.constants.EventConstants;

import io.vertx.core.json.JsonObject;

/**
 * @author szgooru Created On: 27-Feb-2018
 */
public class UserActivity {

    private String eventName;
    private String tenantId;
    private String partnerId;
    private String tenantRoot;
    private String userCategory;
    private String userId;
    private String loginType;
    private long eventEndTime;

    public UserActivity(String eventName, JsonObject userData, Long eventEndTime) {
        this.eventName = eventName;
        this.tenantId = userData.getString(EventConstants.TENANT_ID);
        this.partnerId = userData.getString(EventConstants.PARTNER_ID);
        this.tenantRoot = userData.getString(EventConstants.TENANT_ROOT);
        this.userCategory = userData.getString(EventConstants.USER_CATEGORY);
        this.userId = userData.getString(EventConstants.USER_ID);
        this.loginType = userData.getString(EventConstants.LOGIN_TYPE);

        this.eventEndTime = eventEndTime;
    }

    public String getEventName() {
        return this.eventName;
    }

    public String getTenantId() {
        return this.tenantId;
    }

    public String getPartnerId() {
        return this.partnerId;
    }

    public String getTenantRoot() {
        return this.tenantRoot;
    }

    public String getUserCategory() {
        return this.userCategory;
    }

    public String getUserId() {
        return this.userId;
    }

    public String getLoginType() {
        return this.loginType;
    }

    public long getEventEndTime() {
        return this.eventEndTime;
    }
}
