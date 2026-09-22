package com.github.highcumontoa.taskqueueruntimejava.runtime;

import com.github.highcumontoa.taskqueueruntimejava.model.Credential;
import com.github.highcumontoa.taskqueueruntimejava.model.Permission;

/**
 * 凭据注册与授权检查。无效凭据 -> INVALID_CREDENTIAL；
 * 跨主题/越权 -> CROSS_TOPIC_ACCESS 或 PERMISSION_DENIED，原因可区分。
 */
public interface AccessControl {

    /** 注册凭据；重复 token 抛 TOPIC_ALREADY_EXISTS? 不，抛 BAD_REQUEST。 */
    void register(Credential credential);

    /** 按 token 解析凭据；无效 token 抛 INVALID_CREDENTIAL。 */
    Credential authenticate(String token);

    /** 检查主题级权限；越权抛 PERMISSION_DENIED/CROSS_TOPIC_ACCESS。 */
    void authorize(Credential credential, String topic, Permission permission);

    /** 为某主题授予某凭据指定权限（ACL）。 */
    void grant(String token, String topic, Permission permission);
}
