package com.example.arocketmqdemo.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MsgModel {

    private String orderSn;
    private Integer userId;
    private String desc; // 下单 短信 物流

    // xxx
}
