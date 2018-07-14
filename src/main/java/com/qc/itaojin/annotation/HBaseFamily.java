package com.qc.itaojin.annotation;

import com.qc.itaojin.common.HBaseConstants;

import java.lang.annotation.*;

@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface HBaseFamily {

    String value() default HBaseConstants.DEFAULT_FAMILY;

}
