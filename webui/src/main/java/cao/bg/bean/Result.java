package cao.bg.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author : CGL
 * @Date : 2022/1/10 14:26
 * @Desc : 封装响应给前端的数据的JavaBean(Controller会将该JavaBean转为Json,前端要求该json中有data字段)
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Result {
    private Object data;
    private Integer code;
    private String message;

    public static Result success(Object date) {
        Result result = new Result();
        result.setCode(200);
        result.setMessage("success");
        result.setData(date);
        return result;
    }

    public static Result fail(){
        Result result = new Result();
        result.setCode(500);
        result.setMessage("fail");
        result.setData(null);
        return result;
    }
}
