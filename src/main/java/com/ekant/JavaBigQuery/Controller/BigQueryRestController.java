package com.ekant.JavaBigQuery.Controller;

import com.ekant.JavaBigQuery.Service.BigQueryService;
import com.ekant.JavaBigQuery.model.FlightHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1")
@Slf4j
public class BigQueryRestController {

    @Autowired
    private BigQueryService bigQueryService;

    @GetMapping("/querydb")
    public FlightHolder queryDB() {
        log.info("Starting queryDB");
        return bigQueryService.basicQuery();
    }

    @GetMapping("/insertdb")
    public String insertDB() {
        log.info("Starting insertDB");
        return bigQueryService.insertDB();
    }
}
