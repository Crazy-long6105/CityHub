package com.hmdp.dto;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import lombok.Data;

import java.time.LocalDateTime;

@Data
public class OrderDetailDTO {
    @JsonSerialize(using = ToStringSerializer.class)
    private Long id;
    private Integer status;
    private LocalDateTime payExpireTime;
    private LocalDateTime payTime;
    private boolean releasePending;
}
