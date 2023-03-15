package com.turing.pipeline.example6;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

/**
 * @descri
 *
 * @author lj.michale
 * @date 2023-03-12
 */
@Builder
@Setter
@Getter
public class OrderResult {

    private String orderId;

    private String statu;

    public OrderResult(String orderId, String statu) {

    }
}
