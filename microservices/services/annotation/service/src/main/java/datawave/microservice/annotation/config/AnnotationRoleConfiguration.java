package datawave.microservice.annotation.config;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.util.StringUtils;

/**
 * Selects stream functions after external/bootstrap configuration is available and before any stream properties or bindings are instantiated.
 */
@Configuration(proxyBeanMethods = false)
public class AnnotationRoleConfiguration {
    @Bean
    public static BeanFactoryPostProcessor annotationRoleFunctions(ConfigurableEnvironment environment) {
        return beanFactory -> configureFunctions(environment);
    }

    static void configureFunctions(ConfigurableEnvironment environment) {
        AnnotationRole role = AnnotationRole.from(environment);
        Map<String,Boolean> annotationFunctions = new LinkedHashMap<>();
        annotationFunctions.put("annotationSource", role.includesApi());
        annotationFunctions.put("logAnnotationSink", role.includesWriter() && enabled(environment, "annotation.writers.log.enabled"));
        annotationFunctions.put("accumuloAnnotationSink", role.includesWriter() && enabled(environment, "annotation.writers.accumulo.enabled"));
        annotationFunctions.put("dumpAnnotationSink", role.includesWriter() && enabled(environment, "annotation.writers.dump.enabled"));

        boolean busEnabled = environment.getProperty("spring.cloud.bus.enabled", Boolean.class, true);
        String configured = environment.getProperty("spring.cloud.stream.function.definition");
        if (!StringUtils.hasText(configured)) {
            configured = environment.getProperty("spring.cloud.function.definition");
        }

        List<String> functions;
        if (StringUtils.hasText(configured)) {
            Predicate<String> allowed = name -> annotationFunctions.getOrDefault(name, !"busConsumer".equals(name) || busEnabled);
            // Drop an entire composition if one of its functions is excluded by the role; preserve unrelated compositions.
            functions = Stream.of(configured.split(";")).map(String::trim).filter(StringUtils::hasText)
                            .filter(definition -> Stream.of(definition.split("[|,]")).map(String::trim).allMatch(allowed)).collect(Collectors.toList());
        } else {
            functions = annotationFunctions.entrySet().stream().filter(Map.Entry::getValue).map(Map.Entry::getKey)
                            .collect(Collectors.toCollection(ArrayList::new));
            if (busEnabled) {
                functions.add("busConsumer");
            }
        }

        if (role == AnnotationRole.WRITER && functions.stream().noneMatch(definition -> Stream.of(definition.split("[|,]")).map(String::trim)
                        .anyMatch(name -> !"annotationSource".equals(name) && Boolean.TRUE.equals(annotationFunctions.get(name))))) {
            throw new IllegalStateException("annotation.role=writer requires an enabled and selected log, accumulo, or dump queue consumer");
        }

        Map<String,Object> properties = Map.of("spring.cloud.function.definition", String.join(";", functions), "spring.cloud.stream.function.definition",
                        String.join(";", functions), "spring.cloud.stream.function.autodetect", false);
        environment.getPropertySources().addFirst(new MapPropertySource("annotationRoleFunctions", properties));
    }

    private static boolean enabled(ConfigurableEnvironment environment, String property) {
        return environment.getProperty(property, Boolean.class, false);
    }
}
