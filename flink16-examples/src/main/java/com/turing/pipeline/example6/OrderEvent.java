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
public class OrderEvent {

    public String orderId;

    public long timestamp;

    public String eventType;

    public long lt;

}
