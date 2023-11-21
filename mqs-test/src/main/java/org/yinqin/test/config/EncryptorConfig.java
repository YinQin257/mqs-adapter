package org.yinqin.test.config;

import com.ulisesbocchio.jasyptspringboot.properties.JasyptEncryptorConfigurationProperties;
import org.jasypt.encryption.StringEncryptor;
import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * jasypt自定义加解密配置项算法实现
 *
 * @author YinQin
 * @version 1.0.4
 * @createDate 2023年11月20日
 * @since 1.0.4
 */
@Configuration
@EnableConfigurationProperties({EncryptorProperties.class, JasyptEncryptorConfigurationProperties.class})
public class EncryptorConfig {

    private final Logger logger = LoggerFactory.getLogger(EncryptorConfig.class);

    /**
     * 注入自定义字符串配置项加解密算法生成器
     * @param properties   秘钥配置
     * @return  自定义字符串配置项加解密算法生成器
     */
    @Bean
    StringEncryptor jasyptStringEncryptor(EncryptorProperties properties){
        return new StringEncryptor() {
            @Override
            public String encrypt(String message) {
                return message;
            }

            @Override
            public String decrypt(String encryptedMessage) {
                String decrypt = decryptPBE(encryptedMessage);
                if (logger.isDebugEnabled()) logger.debug("原始密文：{}，自定义密码解密结果：{}",encryptedMessage,decrypt);
                return decrypt;
            }

            private String decryptPBE(String encryptedMessage) {
                StandardPBEStringEncryptor encryptor = new StandardPBEStringEncryptor();
                encryptor.setPassword(properties.getPbeKey());
                return encryptor.decrypt(encryptedMessage);
            }
        };
    }

}
