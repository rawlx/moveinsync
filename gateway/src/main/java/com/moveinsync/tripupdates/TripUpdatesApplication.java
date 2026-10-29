package com.moveinsync.tripupdates;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class TripUpdatesApplication {

    public static void main(String[] args) {
        SpringApplication.run(TripUpdatesApplication.class, args);
    }
}
