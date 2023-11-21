package org.yinqin.test.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * jasypt自定义加解密配置项私钥配置
 *
 * @author YinQin
 * @version 1.0.4
 * @createDate 2023年11月20日
 * @since 1.0.4
 */
@Data
@ConfigurationProperties(prefix = EncryptorProperties.PREFIX)
public class EncryptorProperties {
    public static final String PREFIX = "encryptor";

    private String pbeKey = "e!Jd&ljyJ^e4I5oU";

}
