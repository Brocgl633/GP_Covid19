package cao.bg.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author : CGL
 * @Date : 2022 2022/1/5 19:42
 * @Desc : 用来封装防疫物资的JavaBean
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MaterialBean {
    private String name;//物资名称
    private String from;//物质来源
    private Integer count;//物资数量
}
