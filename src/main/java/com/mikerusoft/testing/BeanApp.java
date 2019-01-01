package com.mikerusoft.testing;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.util.Map;

@SpringBootApplication
@Configuration
@Slf4j
public class BeanApp implements CommandLineRunner {
    public static void main(String[] args) {
        SpringApplication.run(BeanApp.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        System.out.println();

        A aBean = context.getBean(A.class);

        B bBean = context.getBean(B.class);

        Map<String, C> beansOfType = context.getBeansOfType(C.class);
        System.out.println();
    }

    @Bean
    public B getB() {
        return new B1();
    }

    @Autowired
    private ApplicationContext context;

    public static interface A {
        void a();
    }

    public static interface B {
        void b();
    }

    public static interface C {
        void c();
    }

    @Component
    public static class A1 implements A, C {

        @Override
        public void a() {

        }

        @Override
        public void c() {

        }
    }

    public static class B1 implements B, C {

        @Override
        public void b() {

        }

        @Override
        public void c() {

        }
    }
}
