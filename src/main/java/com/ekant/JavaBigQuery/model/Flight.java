package com.ekant.JavaBigQuery.model;

import lombok.*;

@Data
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Flight {
    private String origin;
    private String destination;
    private String airline;
    private Integer stops;
    private Double price;
}
