package com.github.highcumontoa.taskqueueruntimejava.runtime;

import com.github.highcumontoa.taskqueueruntimejava.error.ErrorCode;
import com.github.highcumontoa.taskqueueruntimejava.error.QueueException;
import com.github.highcumontoa.taskqueueruntimejava.model.Credential;
import com.github.highcumontoa.taskqueueruntimejava.model.Permission;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 默认授权器：
 * <ul>
 *   <li>未知/空 token：INVALID_CREDENTIAL；</li>
 *   <li>凭据有效但完全无权访问该主题（不在该主题 ACL 内）：CROSS_TOPIC_ACCESS；</li>
 *   <li>可访问该主题但缺少所需权限：PERMISSION_DENIED。</li>
 * </ul>
 * 每个主题的授权相互独立，拒绝一个主题不影响其他主题的合法访问。
 */
public class DefaultAccessControl implements AccessControl {

    private final ConcurrentHashMap<String, Credential> credentials = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Set<Permission>> acl = new ConcurrentHashMap<>();

    private static String key(String token, String topic) {
        return token + "|" + topic;
    }

    @Override
    public void register(Credential credential) {
        if (credential == null || credential.getToken() == null || credential.getToken().isBlank()) {
            throw new QueueException(ErrorCode.BAD_REQUEST, "凭据与 token 不能为空");
        }
        if (credentials.putIfAbsent(credential.getToken(), credential) != null) {
            throw new QueueException(ErrorCode.BAD_REQUEST, "凭据已存在: " + credential.getToken());
        }
    }

    @Override
    public Credential authenticate(String token) {
        if (token == null || token.isBlank()) {
            throw new QueueException(ErrorCode.INVALID_CREDENTIAL, "缺少凭据 token");
        }
        Credential credential = credentials.get(token);
        if (credential == null) {
            throw new QueueException(ErrorCode.INVALID_CREDENTIAL, "无效凭据: token 无法识别");
        }
        return credential;
    }

    @Override
    public void authorize(Credential credential, String topic, Permission permission) {
        if (credential.getGlobalPermissions().contains(Permission.ADMIN)) {
            return;
        }
        Set<Permission> topicPermissions = acl.get(key(credential.getToken(), topic));
        if (topicPermissions == null) {
            throw new QueueException(ErrorCode.CROSS_TOPIC_ACCESS,
                    "凭据 '" + credential.getPrincipal() + "' 未被授权访问主题: " + topic);
        }
        if (!topicPermissions.contains(permission)) {
            throw new QueueException(ErrorCode.PERMISSION_DENIED,
                    "凭据 '" + credential.getPrincipal() + "' 对主题 '" + topic
                            + "' 缺少权限: " + permission);
        }
    }

    @Override
    public void grant(String token, String topic, Permission permission) {
        if (!credentials.containsKey(token)) {
            throw new QueueException(ErrorCode.INVALID_CREDENTIAL, "无法对未知凭据授权: " + token);
        }
        acl.compute(key(token, topic), (k, existing) -> {
            Set<Permission> next = existing == null
                    ? EnumSet.noneOf(Permission.class)
                    : EnumSet.copyOf(existing);
            next.add(permission);
            return Collections.unmodifiableSet(next);
        });
    }
}
