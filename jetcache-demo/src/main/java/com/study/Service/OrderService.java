package com.study.Service;

import com.alicp.jetcache.Cache;
import com.alicp.jetcache.CacheGetResult;
import com.alicp.jetcache.CacheManager;
import com.alicp.jetcache.anno.*;
import com.study.pojo.Order;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
public class OrderService {

    private static final Logger logger = LoggerFactory.getLogger(OrderService.class);

    private int couter;

    @Autowired
    private CacheManager cacheManager;

    //    @CreateCache(name = "orderCache", syncLocal = true, expire = 30, timeUnit = TimeUnit.SECONDS, cacheType = CacheType.BOTH)
    private Cache<String, Order> orderCache;

    @PostConstruct
    public void init() {
//        QuickConfig qc = QuickConfig.newBuilder("orderCache")
//                .expire(Duration.ofSeconds(100))
//                .cacheType(CacheType.LOCAL) // two level cache
//                .syncLocal(true) // invalidate local cache in all jvm process after update
//                .build();
//        orderCache = cacheManager.getOrCreateCache(qc);
    }


    @Cached(name = "order.",
            key = "#orderParam.name",
            localExpire = 200,
            syncLocal = true, cacheType = CacheType.BOTH, expire = 300, timeUnit = TimeUnit.SECONDS)
    // CacheRefresh 是根据key来缓存的, key太多, hashmap里面放的对象就太多
//    @CacheRefresh(refresh = 3,
////            stopRefreshAfterLastAccess = 3,
//            timeUnit = TimeUnit.SECONDS,
//            refreshLockTimeout = 5)
    @CachePenetrationProtect(value = true, timeout = 2)
    public Order queryOrderOfLocal(Order orderParam) {

        logger.info("===================== queryOrderOfLocal =====================");

        Order order = new Order();
        if (couter % 2 == 0) {
            order.setName("2");
        } else {
            order.setName("1");
        }

        couter = couter + 1;
        return order;
    }

    @CacheInvalidate(name = "order.",
            key = "#orderParam.name")
    public void invalidate(Order orderParam) {

    }


    public Order useCacheApi(Order orderParam) {

        logger.info("===================== useCacheApi =====================");

        orderCache.put("key11", orderParam);
        Order res = (Order) orderCache.get("key11");

        CacheGetResult<Order> key11 = orderCache.GET("key11");
        key11.future().thenRun(() -> {
            if (key11.isSuccess()) {
                System.out.println(key11.getValue());
            }
        });

        return res;
    }

}
