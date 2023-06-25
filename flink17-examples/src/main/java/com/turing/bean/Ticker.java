package com.turing.bean;

import java.time.LocalDateTime;
import java.util.Date;

/**
 * @descri
 *
 * @author lj.michale
 * @date 2023-03-14
 */
public class Ticker {

    private Integer id;

    private String symbol;

    private Integer price;

    private Integer tax;

    private LocalDateTime rowtime;

    public Ticker(Integer id, String symbol, Integer price, Integer tax, LocalDateTime rowtime) {
        this.id = id;
        this.symbol = symbol;
        this.price = price;
        this.tax = tax;
        this.rowtime = rowtime;
    }
}
