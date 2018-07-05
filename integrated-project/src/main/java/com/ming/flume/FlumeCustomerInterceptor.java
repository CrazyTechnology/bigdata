package com.ming.flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;

public class FlumeCustomerInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        byte[] body = event.getBody();
        return null;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        for (Event event : events){

            intercept(event);
        }
        return events;
    }

    @Override
    public void close() {

    }


    public static class Builder implements Interceptor.Builder
    {
        @Override
        public void configure(Context context) {
            // TODO Auto-generated method stub
        }

        @Override
        public Interceptor build() {
            return new FlumeCustomerInterceptor();
        }
    }
}
