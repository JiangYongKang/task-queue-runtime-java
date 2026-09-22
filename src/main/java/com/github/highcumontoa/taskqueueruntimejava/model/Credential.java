package com.github.highcumontoa.taskqueueruntimejava.model;

import java.util.Set;

/**
 * 调用方凭据：token 标识身份，permissions 为全局权限集合。
 * 主题级 ACL 在凭据的 topicPermissions 中声明（主题 -> 权限集合）。
 */
public final class Credential {
    private final String token;
    private final String principal;
    private final Set<Permission> globalPermissions;

    public Credential(String token, String principal, Set<Permission> globalPermissions) {
        this.token = token;
        this.principal = principal;
        this.globalPermissions = Set.copyOf(globalPermissions);
    }

    public String getToken() { return token; }
    public String getPrincipal() { return principal; }
    public Set<Permission> getGlobalPermissions() { return globalPermissions; }
}
