package com.study.controller;

import com.study.Service.OrderService;
import com.study.pojo.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Optional;

@RestController
public class TestController {

    private static final Logger logger = LoggerFactory.getLogger(TestController.class);

    @Autowired
    private OrderService orderService;

    /**
     * http://127.0.0.1:8083/demo?name=211 测试
     */
    @RequestMapping("/demo")
    public String demo(@RequestParam(value = "name", defaultValue = "World") String name) {

        logger.info("===================== demo =====================");

        Order order = orderService.createOrder(new Order(1, name));
//        Order order = orderService.createOrder();
        return Optional.ofNullable(order).map(x -> x.toString()).orElse(null);
    }


    /**
     * http://127.0.0.1:8083/test?name=398 测试
     */
    @RequestMapping("/test")
    public String test(@RequestParam(value = "name", defaultValue = "World") String name) {

        logger.info("===================== test =====================");

        Order order = orderService.useCacheApi(new Order(22222, name));
//        Order order = orderService.createOrder();
        return Optional.ofNullable(order).map(x -> x.toString()).orElse(null);
    }

}
