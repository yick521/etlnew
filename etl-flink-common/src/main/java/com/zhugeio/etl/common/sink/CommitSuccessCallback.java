package com.zhugeio.etl.common.sink;

import java.io.Serializable;

/**
 * Commit 成功回调接口
 * 
 * 用于在 Doris 2PC commit 成功后通知外部系统
 */
@FunctionalInterface
public interface CommitSuccessCallback extends Serializable {
    
    /**
     * commit 成功后回调
     * 
     * @param tableName 表名
     * @param recordCount 本次 commit 成功的记录数
     */
    void onCommitSuccess(String tableName, long recordCount);
}
