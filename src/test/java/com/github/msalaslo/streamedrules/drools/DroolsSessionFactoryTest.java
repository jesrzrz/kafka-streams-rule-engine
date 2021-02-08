package com.github.msalaslo.streamedrules.drools;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;

import com.github.msalaslo.streamedrules.model.Message;

public class DroolsSessionFactoryTest {

    @Test
    public void testDroolsSessionInstantiated() {
		KieServices ks = KieServices.Factory.get();
		KieContainer kContainer = ks.getKieClasspathContainer();
		KieSession kieSession = kContainer.newKieSession("IfContainsEPrepend0KS");
		
        Message message = new Message("hello");
        kieSession.insert(message);
        kieSession.fireAllRules();
        assertEquals("0hello", message.getContent());
    }
}