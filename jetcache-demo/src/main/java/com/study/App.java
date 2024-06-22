package com.study;

import com.alicp.jetcache.anno.config.EnableCreateCacheAnnotation;
import com.alicp.jetcache.anno.config.EnableMethodCache;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {"com.study"})
/**
 * jetcache spring 自动装配
 *  {@link com.alicp.jetcache.autoconfigure.JetCacheAutoConfiguration}
 */
@EnableMethodCache(basePackages = "com.study")
//@EnableCreateCacheAnnotation
public class App {

    public static void main(String[] args) {

        SpringApplication.run(App.class, args);
    }
}
