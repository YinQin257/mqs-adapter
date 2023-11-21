package org.yinqin.test;

import com.ulisesbocchio.jasyptspringboot.annotation.EnableEncryptableProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableEncryptableProperties
public class MqAdapterTestApplication {
    public static void main(String[] args) {
        SpringApplication.run(MqAdapterTestApplication.class, args);
    }
}
