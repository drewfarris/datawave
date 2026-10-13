package datawave.microservice.annotation.service.config;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.security.web.firewall.StrictHttpFirewall;

import datawave.microservice.annotation.common.AnnotationSupplier;
import datawave.microservice.annotation.common.config.AnnotationJsonConfiguration;
import datawave.microservice.annotation.config.AnnotationRole;
import datawave.microservice.annotation.config.ConditionalOnAnnotationRole;

@Configuration
@EnableConfigurationProperties(AnnotationProperties.class)
@ConditionalOnAnnotationRole({AnnotationRole.API, AnnotationRole.COMBINED})
@Import(AnnotationJsonConfiguration.class)
public class AnnotationServiceConfig {
    @Bean
    public AnnotationSupplier annotationSource() {
        return new AnnotationSupplier();
    }

    @Bean
    public StrictHttpFirewall httpFirewall() {
        // override the default strict http firewall to allow forward slashes. These are used in internal identifiers
        // as separators for shard/datatype/uid structures. We also allow shard:datatype:uid, but in some cases we
        // want to accept the slash variant which is returned by other services.
        StrictHttpFirewall firewall = new StrictHttpFirewall();
        firewall.setAllowUrlEncodedSlash(true);
        // this is due to strangeness in the StrictHttpFirewall, you must also allow encoded percents to allow encoded
        // slashes, arrived here via testing.
        firewall.setAllowUrlEncodedPercent(true);
        return firewall;
    }

    @Bean(destroyMethod = "shutdown")
    public ExecutorService federatedReadExecutorService() {
        return Executors.newCachedThreadPool();
    }
}
