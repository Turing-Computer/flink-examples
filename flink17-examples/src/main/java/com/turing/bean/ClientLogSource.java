package com.turing.bean;

import lombok.Builder;
import lombok.Data;

/**
 * @descri
 *
 * @author lj.michale
 * @date 2023-03-07
 */
@Data
@Builder
public class ClientLogSource {

    private int id;

    private int price;

    private long timestamp;

    private String date;

    private String page;

}