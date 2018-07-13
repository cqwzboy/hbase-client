package com.qc.itaojin.annotation;

import java.lang.annotation.*;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface EnableHBaseTableCheck {

    String[] scan() default "";

}
