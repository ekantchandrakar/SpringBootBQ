package com.ekant.JavaBigQuery.model;

import lombok.Data;

import java.util.List;

@Data
public class FlightHolder {
    private Integer count;
    private String status;
    private List<Flight> flights;
}
