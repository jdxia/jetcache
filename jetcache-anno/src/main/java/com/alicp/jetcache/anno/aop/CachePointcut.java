/**
 * Created on  13-09-19 20:56
 */
package com.alicp.jetcache.anno.aop;

import com.alicp.jetcache.anno.method.CacheConfigUtil;
import com.alicp.jetcache.anno.method.CacheInvokeConfig;
import com.alicp.jetcache.anno.method.ClassUtil;
import com.alicp.jetcache.anno.support.ConfigMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.ClassFilter;
import org.springframework.aop.support.StaticMethodMatcherPointcut;
import org.springframework.asm.Type;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

/**
 * @author huangli
 */
public class CachePointcut extends StaticMethodMatcherPointcut implements ClassFilter {

    private static final Logger logger = LoggerFactory.getLogger(CachePointcut.class);

    private ConfigMap cacheConfigMap;
    private String[] basePackages;

    /**
     * 构造函数
     *
     * @param basePackages 基础包名数组
     */
    public CachePointcut(String[] basePackages) {
        setClassFilter(this);
        this.basePackages = basePackages;
    }

    /**
     * 判断类是否匹配
     *
     * @param clazz 类对象
     * @return 是否匹配
     */
    @Override
    public boolean matches(Class clazz) {
        boolean b = matchesImpl(clazz);
        logger.trace("check class match {}: {}", b, clazz);
        return b;
    }

    /**
     * 实现类是否匹配
     *
     * @param clazz 类对象
     * @return 是否匹配
     */
    private boolean matchesImpl(Class clazz) {
        if (matchesThis(clazz)) {
            return true;
        }
        Class[] cs = clazz.getInterfaces();
        if (cs != null) {
            for (Class c : cs) {
                if (matchesImpl(c)) {
                    return true;
                }
            }
        }
        if (!clazz.isInterface()) {
            Class sp = clazz.getSuperclass();
            if (sp != null && matchesImpl(sp)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 判断类是否匹配
     *
     * @param clazz 类对象
     * @return 是否匹配
     */
    public boolean matchesThis(Class clazz) {
        String name = clazz.getName();
        if (exclude(name)) {
            return false;
        }
        return include(name);
    }

    /**
     * 判断类是否包含在基础包名数组中
     *
     * @param name 类名
     * @return 是否包含
     */
    private boolean include(String name) {
        if (basePackages != null) {
            for (String p : basePackages) {
                if (name.startsWith(p)) {
                    return true;
                }
            }
        }
        return false;
    }

    // 这些要排除掉
    private boolean exclude(String name) {
        if (name.startsWith("java")) {
            return true;
        }
        if (name.startsWith("org.springframework")) {
            return true;
        }
        if (name.indexOf("$$EnhancerBySpringCGLIB$$") >= 0) {
            return true;
        }
        if (name.indexOf("$$FastClassBySpringCGLIB$$") >= 0) {
            return true;
        }
        return false;
    }

    /**
     * 判断给定的方法是否与目标类匹配
     *
     * @param method 待判断的方法
     * @param targetClass 目标类
     * @return 如果匹配则返回true，否则返回false
     */
    @Override
    public boolean matches(Method method, Class targetClass) {
        boolean b = matchesImpl(method, targetClass);
        if (b) {
            if (logger.isDebugEnabled()) {
                logger.debug("check method match true: method={}, declaringClass={}, targetClass={}",
                        method.getName(),
                        ClassUtil.getShortClassName(method.getDeclaringClass().getName()),
                        targetClass == null ? null : ClassUtil.getShortClassName(targetClass.getName()));
            }
        } else {
            if (logger.isTraceEnabled()) {
                logger.trace("check method match false: method={}, declaringClass={}, targetClass={}",
                        method.getName(),
                        ClassUtil.getShortClassName(method.getDeclaringClass().getName()),
                        targetClass == null ? null : ClassUtil.getShortClassName(targetClass.getName()));
            }
        }
        return b;
    }

    /**
     * 实现方法匹配的逻辑
     * @param method 待判断的方法
     * @param targetClass 目标类
     * @return 如果匹配则返回true，否则返回false
     *
     *
     * 1、调用matchesThis函数判断指定的类的包名是否以配置的JetCache扫描的包路径开头，这是为了排除没有JetCache不会扫描的类。
     * 2、调用exclude函数用于排除Java和Spring自带的类名，以及使用CGLIB动态代理生成的类。
     * 3、根据类和函数信息生成唯一键Key，然后查询缓存cacheConfigMap中对应的缓存调用配置信息。
     * 4、如果cacheConfigMap缺少指定类的指定函数的缓存调用配置信息，则构建CacheInvokeConfig实例，并解析函数的注解信
     */
    private boolean matchesImpl(Method method, Class targetClass) {
        if (!matchesThis(method.getDeclaringClass())) {
            return false;
        }
        if (exclude(targetClass.getName())) {
            return false;
        }
        String key = getKey(method, targetClass);
        CacheInvokeConfig cac = cacheConfigMap.getByMethodInfo(key);
        if (cac == CacheInvokeConfig.getNoCacheInvokeConfigInstance()) {
            return false;
        } else if (cac != null) {
            return true;
        } else {
            cac = new CacheInvokeConfig();
            CacheConfigUtil.parse(cac, method);

            String name = method.getName();
            Class<?>[] paramTypes = method.getParameterTypes();
            parseByTargetClass(cac, targetClass, name, paramTypes);

            if (!cac.isEnableCacheContext() && cac.getCachedAnnoConfig() == null &&
                    cac.getInvalidateAnnoConfigs() == null && cac.getUpdateAnnoConfig() == null) {
                cacheConfigMap.putByMethodInfo(key, CacheInvokeConfig.getNoCacheInvokeConfigInstance());
                return false;
            } else {
                cacheConfigMap.putByMethodInfo(key, cac);
                return true;
            }
        }
    }

    public static String getKey(Method method, Class targetClass) {
        StringBuilder sb = new StringBuilder();
        sb.append(method.getDeclaringClass().getName());
        sb.append('.');
        sb.append(method.getName());
        sb.append(Type.getMethodDescriptor(method));
        if (targetClass != null) {
            sb.append('_');
            sb.append(targetClass.getName());
        }
        return sb.toString();
    }

    private void parseByTargetClass(CacheInvokeConfig cac, Class<?> clazz, String name, Class<?>[] paramTypes) {
        if (!clazz.isInterface() && clazz.getSuperclass() != null) {
            parseByTargetClass(cac, clazz.getSuperclass(), name, paramTypes);
        }
        Class<?>[] intfs = clazz.getInterfaces();
        for (Class<?> it : intfs) {
            parseByTargetClass(cac, it, name, paramTypes);
        }

        boolean matchThis = matchesThis(clazz);
        if (matchThis) {
            Method[] methods = clazz.getDeclaredMethods();
            for (Method method : methods) {
                if (methodMatch(name, method, paramTypes)) {
                    // 会解析函数上的Cached、CacheUpdate和CacheInvalidate注解上的信息
                    CacheConfigUtil.parse(cac, method);
                    break;
                }
            }
        }
    }

    private boolean methodMatch(String name, Method method, Class<?>[] paramTypes) {
        if (!Modifier.isPublic(method.getModifiers())) {
            return false;
        }
        if (!name.equals(method.getName())) {
            return false;
        }
        Class<?>[] ps = method.getParameterTypes();
        if (ps.length != paramTypes.length) {
            return false;
        }
        for (int i = 0; i < ps.length; i++) {
            if (!ps[i].equals(paramTypes[i])) {
                return false;
            }
        }
        return true;
    }


    public void setCacheConfigMap(ConfigMap cacheConfigMap) {
        this.cacheConfigMap = cacheConfigMap;
    }
}
