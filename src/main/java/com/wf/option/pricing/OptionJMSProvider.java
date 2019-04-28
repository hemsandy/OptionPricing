package com.wf.option.pricing;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import org.apache.storm.jms.JmsProvider;

/**
 * Created by hems on 19/04/19.
 */
public class OptionJMSProvider implements JmsProvider{

    private ConnectionFactory connectionFactory;
    private Destination destination;

    public OptionJMSProvider(ApplicationContext appContext,
                             String connectionFactoryBean, String destinationBean){
        this.connectionFactory = (ConnectionFactory)appContext.getBean(connectionFactoryBean);
        this.destination = (Destination)appContext.getBean(destinationBean);
    }

    public ConnectionFactory connectionFactory() throws Exception {
        return this.connectionFactory;
    }

    public Destination destination() throws Exception {
        return this.destination;
    }
}
