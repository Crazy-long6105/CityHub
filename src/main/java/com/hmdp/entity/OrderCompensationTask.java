package com.hmdp.entity;

import lombok.Data;

import java.time.LocalDateTime;

@Data
public class OrderCompensationTask {
    private Long id;
    private Long orderId;
    private Long userId;
    private Long voucherId;
    private String taskType;
    private Integer status;
    private Integer retryCount;
    private LocalDateTime nextExecuteTime;
    private String claimToken;
    private LocalDateTime leaseUntil;
    private String lastError;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
    private LocalDateTime completedTime;
}
