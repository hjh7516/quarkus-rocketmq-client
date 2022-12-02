package io.quarkiverse.rocketmq.test.resorurce.entity;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AccessLevel;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

import java.io.Serializable;

/**
 * 统一的业务返回包装类
 *
 * @author qingzhi
 */
@Data
@Accessors(chain = true)
@RequiredArgsConstructor(staticName = "of", access = AccessLevel.PRIVATE)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class BizResult<T> implements Serializable {

    private static final long serialVersionUID = -1702542373929399568L;

    private boolean success = false;
    private Integer code;
    private String msg;
    private T data;

    /**
     * 正常返回
     *
     * @param data 数据内容
     * @param <T> 返回数据的类型
     * @return 包装后的返回
     */
    public static <T> BizResult<T> create(T data) {
        return BizResult.<T>of().setSuccess(true).setData(data);
    }

    /**
     * 错误返回
     */
    public static <T> BizResult<T> error(Integer code, String msg) {
        return BizResult.<T>of().setSuccess(false).setCode(code).setMsg(msg);
    }

//    /**
//     * 错误返回
//     */
//    public static <T> BizResult<T> error(EnumInterface<? extends EnumInterface<?>> errorCodeEnum) {
//        return BizResult.<T>error(errorCodeEnum.getCode(), errorCodeEnum.getDesc());
//    }
//
//    /**
//     * 错误返回
//     */
//    public static <T> BizResult<T> error(EnumInterface<? extends EnumInterface<?>> errorCodeEnum, String msg) {
//        return BizResult.<T>error(errorCodeEnum.getCode(), msg);
//    }
//
//    /**
//     * 错误返回
//     */
//    public static <T> BizResult<T> error(ExceptionInterface ex) {
//        return BizResult.<T>error(ex.getCode(), ex.getMsg());
//    }

}
