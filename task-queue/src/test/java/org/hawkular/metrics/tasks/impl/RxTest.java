/*
 * Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hawkular.metrics.tasks.impl;

import static java.util.Arrays.asList;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import rx.Observable;
import rx.schedulers.Schedulers;

/**
 * @author jsanda
 */
public class RxTest {

    private static final Logger logger = LoggerFactory.getLogger(RxTest.class);

//    @Test
    public void groupBy() {
        Observable<Integer> numbers = Observable.from(asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        numbers.groupBy(num -> num % 2 == 0)
                .flatMap(group -> group.map(num -> Integer.toString(num)))
                .subscribe(System.out::println, System.err::println);
    }

//    @Test
    public void errors() {
        Observable.from(asList(1, 2, 3, 4, 5))
                .map(i -> Integer.toString(i))
                .map(s -> {
                    if (s.equals("3")) {
                        throw new RuntimeException("BAM!");
                    }
                    return "Number " + s;
                })
                .map(String::toUpperCase)
                .subscribe(System.out::println, Throwable::printStackTrace, () -> System.out.println("Done!"));
    }

    @Test
    public void concurrency() throws Exception {
        DataService service = new DataService();
        ScheduledExecutorService threadPool = Executors.newScheduledThreadPool(2);
        threadPool.scheduleAtFixedRate(() -> {
                    logger.info("Fetching data");
                    service.findData().subscribe(
                            data -> logger.info("Data = {}", data),
                            t -> logger.error("Failed to fetch data", t)
                    );
                },
                0,
                100,
                TimeUnit.MILLISECONDS);
        Thread.sleep(3000);
        threadPool.shutdownNow();
    }

    static class DataService {
        static final Logger logger = LoggerFactory.getLogger(DataService.class);

        public Observable<Long> findData() {
            Observable<Long> data = Observable.create(subscriber -> {
                try {
                    Thread.sleep(100);
                    logger.info("Sending data");
                    subscriber.onNext(System.currentTimeMillis());
                    subscriber.onCompleted();
                } catch (Exception e) {
                    subscriber.onError(e);
                }
            });
            return data.subscribeOn(Schedulers.io());
        }
    }

}
