package com.github.highcumontoa.taskqueueruntimejava.queue.model;

import java.util.Set;

/**
 * 凭据主体。ADMIN 权限隐含所有主题上的全部权限。
 * 非管理员仅可访问 {@code grantedTopics}（可含 "*" 通配）。
 */
public record Principal(String token, Set<Permission> permissions, Set<String> grantedTopics) {

    public Principal {
        permissions = permissions == null ? Set.of() : Set.copyOf(permissions);
        grantedTopics = grantedTopics == null ? Set.of() : Set.copyOf(grantedTopics);
    }

    public boolean isAdmin() {
        return permissions.contains(Permission.ADMIN);
    }

    public boolean hasPermission(Permission p) {
        return isAdmin() || permissions.contains(p);
    }

    /** 主题授权：管理员隐式通过；其余按显式授权或通配符判定。 */
    public boolean topicGranted(String topic) {
        if (isAdmin()) {
            return true;
        }
        return grantedTopics.contains(topic) || grantedTopics.contains("*");
    }
}
