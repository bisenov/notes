package com.mysite.core.dispatcher;

import com.day.cq.replication.ReplicationAction;
import com.mysite.core.dispatcher.FlushingDispEventHandlerRegister.Configuration;
import com.mysite.core.util.ParameterUtil;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.osgi.framework.BundleContext;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.event.EventConstants;
import org.osgi.service.event.EventHandler;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component(
    immediate = true,
    configurationPolicy = ConfigurationPolicy.OPTIONAL
    )
@Designate(ocd = Configuration.class, factory = true)
public class FlushingDispEventHandlerRegister {

    @ObjectClassDefinition(
        name = "Dispatcher cache flushing service",
        description = "Service to register flushing dispatcher cache event handlers")
    @interface Configuration {

        @AttributeDefinition(
            name = "Flush Rules (ResourceOnly)",
            description = "LDAP-style filter syntax pattern to Path associations for flush rules. "
                + "Format: <pattern-of-trigger-content>=<path-to-flush> "
                + "Example: /content/mysite/triggeredcontent/*=/content/mysite/flushedcontent/somepage "
                + "& as a separator for multiple flushed paths.",
            cardinality = Integer.MAX_VALUE)
        String[] rules();
    }

    @Reference
    private ResourceResolverFactory resourceResolverFactory;
    @Reference
    private DispatcherFlusher dispatcherFlusher;

    private final Logger log = LoggerFactory.getLogger(FlushingDispEventHandlerRegister.class);
    private final List<ServiceRegistration<?>> serviceRegistrations = new ArrayList<>();

    private static final String SERVICE_NAME = "dispatcher-flush";
    final Map<String, Object>
    AUTH_INFO = Collections.singletonMap(ResourceResolverFactory.SUBSERVICE, SERVICE_NAME);

    @Activate
    protected final void activate(final Configuration configuration) {
        Map<String, String[]> flushRules = this.configureFlushRules(
            ParameterUtil.toMap(configuration.rules(), "="));
        String rules = log.isDebugEnabled() ? Arrays.toString(configuration.rules()) : null;
        log.debug("Class has been activated with rules: {}", rules);

        try (ResourceResolver resourceResolver = resourceResolverFactory.getServiceResourceResolver(AUTH_INFO)) {
            flushRules.forEach((k, v) -> serviceRegistrations.add(registerHandler(k, v, resourceResolver)));
        }
        catch (LoginException e) {
            log.error("Error issuing dispatcher flush rules due to a repository login exception: {}", e.getMessage(), e);
        }
    }

    private ServiceRegistration<?> registerHandler(String triggeredPaths, String[] flushedPaths, ResourceResolver resolver) {
        Dictionary<String, String> properties = new Hashtable<>();
        properties.put(EventConstants.EVENT_TOPIC, ReplicationAction.EVENT_TOPIC);
        properties.put(EventConstants.EVENT_FILTER, "(paths=" + triggeredPaths +")");

        log.debug("Registering handler with event filter: {}", triggeredPaths);
        return getBundleContext().registerService(
            EventHandler.class.getName(),
            new FlushingDispEventHandler(flushedPaths, resolver, dispatcherFlusher),
            properties);

    }

    @Deactivate
    protected final void deactivate() {
        serviceRegistrations.forEach(ServiceRegistration::unregister);
        log.debug("Component was deactivated, {} services has been unregistered", serviceRegistrations.size());
    }

    protected final Map<String, String[]> configureFlushRules(final Map<String, String> configuredRules) {

        return configuredRules.entrySet()
                              .stream().map(e -> new SimpleImmutableEntry<>(
                                  e.getKey().trim(),
                                  e.getValue().trim().split("&")))
                              .collect(Collectors.toMap(SimpleImmutableEntry::getKey, SimpleImmutableEntry::getValue));
    }

    private BundleContext getBundleContext() {
        return FrameworkUtil.getBundle(this.getClass()).getBundleContext();
    }
}
