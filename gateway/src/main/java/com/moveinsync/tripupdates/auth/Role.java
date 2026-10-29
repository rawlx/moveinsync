package com.moveinsync.tripupdates.auth;

/** Roles of authenticated WebSocket clients. Each role's topic rules are in {@link TopicAuthorizer}. */
public enum Role {
    /** May read only the trips they ride on. */
    EMPLOYEE,
    /** May read the office topics, and trips and cabs, of the offices they administer. */
    OFFICE_ADMIN,
    /** May read only the SOS topics explicitly granted to them. */
    SECURITY
}
