/*
 * Copyright 2014-2017 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.core.operators;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.hawkular.metrics.core.service.transformers.SortedMerge;
import org.junit.Test;

import rx.Observable;
import rx.observers.TestSubscriber;
import rx.schedulers.Schedulers;


/**
 * Originally from https://gist.github.com/akarnokd/c86a89738199bbb37348
 *
 * Added tests for duplicate filtering feature added in our version of SortedMerge
 *
 * @author Michael Burman
 */
public class SortedMergeTest {

    @Test
    public void testSymmetricMerge() {
        Observable<Integer> o1 = Observable.just(1, 3, 5, 7);
        Observable<Integer> o2 = Observable.just(2, 4, 6, 8);

        TestSubscriber<Integer> ts = new TestSubscriber<>();
        SortedMerge.create(Arrays.asList(o1, o2)).subscribe(ts);

        ts.assertNoErrors();
        ts.assertCompleted();
        ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8);
    }

    @Test
    public void testSymmetricDuplicateRemovalMerge() {
        Observable<Integer> o1 = Observable.just(1, 4, 5, 7);
        Observable<Integer> o2 = Observable.just(2, 4, 6, 8);

        TestSubscriber<Integer> ts = new TestSubscriber<>();
        SortedMerge.create(Arrays.asList(o1, o2), false)
                .distinctUntilChanged((integer, integer2) -> integer.compareTo(integer2) == 0)
                .subscribe(ts);

        ts.assertNoErrors();
        ts.assertCompleted();
        ts.assertValues(1, 2, 4, 5, 6, 7, 8);
    }

    @Test
    public void testAsymmetricMerge() {
        Observable<Integer> o1 = Observable.just(1, 3, 5, 7);
        Observable<Integer> o2 = Observable.just(2, 4);

        TestSubscriber<Integer> ts = new TestSubscriber<>();
        SortedMerge.create(Arrays.asList(o1, o2)).subscribe(ts);

        ts.assertNoErrors();
        ts.assertCompleted();
        ts.assertValues(1, 2, 3, 4, 5, 7);
    }

    @Test
    public void testAsymmetricDuplicateRemovalMerge() {
        Observable<Integer> o1 = Observable.just(1, 3, 5, 7);
        Observable<Integer> o2 = Observable.just(2, 3);

        TestSubscriber<Integer> ts = new TestSubscriber<>();
        SortedMerge.create(Arrays.asList(o1, o2), false)
                .distinctUntilChanged((integer, integer2) -> integer.compareTo(integer2) == 0)
                .subscribe(ts);

        ts.assertNoErrors();
        ts.assertCompleted();
        ts.assertValues(1, 2, 3, 5, 7);
    }

    @Test
    public void testSymmetricMergeAsync() {
        Observable<Integer> o1 = Observable.just(1, 3, 5, 7).observeOn(Schedulers.computation());
        Observable<Integer> o2 = Observable.just(2, 4, 6, 8).observeOn(Schedulers.computation());

        TestSubscriber<Integer> ts = new TestSubscriber<>();
        SortedMerge.create(Arrays.asList(o1, o2)).subscribe(ts);

        ts.awaitTerminalEvent(1, TimeUnit.SECONDS);
        ts.assertNoErrors();
        ts.assertCompleted();
        ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8);
    }

    @Test
    public void testSymmetricDuplicateRemovalMergeAsync() {
        Observable<Integer> o1 = Observable.just(1, 4, 5, 7).observeOn(Schedulers.computation());
        Observable<Integer> o2 = Observable.just(2, 4, 6, 8).observeOn(Schedulers.computation());

        TestSubscriber<Integer> ts = new TestSubscriber<>();
        SortedMerge.create(Arrays.asList(o1, o2), false)
                .distinctUntilChanged((integer, integer2) -> integer.compareTo(integer2) == 0)
                .subscribe(ts);

        ts.awaitTerminalEvent(1, TimeUnit.SECONDS);
        ts.assertNoErrors();
        ts.assertCompleted();
        ts.assertValues(1, 2, 4, 5, 6, 7, 8);
    }

    @Test
    public void testAsymmetricMergeAsync() {
        Observable<Integer> o1 = Observable.just(1, 3, 5, 7).observeOn(Schedulers.computation());
        Observable<Integer> o2 = Observable.just(2, 4).observeOn(Schedulers.computation());

        TestSubscriber<Integer> ts = new TestSubscriber<>();
        SortedMerge.create(Arrays.asList(o1, o2)).subscribe(ts);

        ts.awaitTerminalEvent(1, TimeUnit.SECONDS);
        ts.assertNoErrors();
        ts.assertCompleted();
        ts.assertValues(1, 2, 3, 4, 5, 7);
    }

    @Test
    public void testAsymmetricDuplicateRemovalMergeAsync() {
        Observable<Integer> o1 = Observable.just(1, 3, 5, 7).observeOn(Schedulers.computation());
        Observable<Integer> o2 = Observable.just(2, 3).observeOn(Schedulers.computation());

        TestSubscriber<Integer> ts = new TestSubscriber<>();
        SortedMerge.create(Arrays.asList(o1, o2), false)
                .distinctUntilChanged((integer, integer2) -> integer.compareTo(integer2) == 0)
                .subscribe(ts);

        ts.awaitTerminalEvent(1, TimeUnit.SECONDS);
        ts.assertNoErrors();
        ts.assertCompleted();
        ts.assertValues(1, 2, 3, 5, 7);
    }

    @Test
    public void testEmptyEmpty() {
        Observable<Integer> o1 = Observable.empty();
        Observable<Integer> o2 = Observable.empty();

        TestSubscriber<Integer> ts = new TestSubscriber<>();
        SortedMerge.create(Arrays.asList(o1, o2)).subscribe(ts);

        ts.assertNoErrors();
        ts.assertCompleted();
        ts.assertNoValues();
    }

    @Test
    public void testEmptySomething1() {
        Observable<Integer> o1 = Observable.just(1, 3, 5, 7);
        Observable<Integer> o2 = Observable.empty();

        TestSubscriber<Integer> ts = new TestSubscriber<>();
        SortedMerge.create(Arrays.asList(o1, o2)).subscribe(ts);

        ts.assertNoErrors();
        ts.assertCompleted();
        ts.assertValues(1, 3, 5, 7);
    }

    @Test
    public void testEmptySomething2() {
        Observable<Integer> o1 = Observable.just(1, 3, 5, 7);
        Observable<Integer> o2 = Observable.empty();

        TestSubscriber<Integer> ts = new TestSubscriber<>();
        SortedMerge.create(Arrays.asList(o2, o1)).subscribe(ts);

        ts.assertNoErrors();
        ts.assertCompleted();
        ts.assertValues(1, 3, 5, 7);
    }

    @Test
    public void testEmptySomethingNoDuplicates() {
        Observable<Integer> o1 = Observable.just(1, 3, 5, 7);
        Observable<Integer> o2 = Observable.empty();

        TestSubscriber<Integer> ts = new TestSubscriber<>();
        SortedMerge.create(Arrays.asList(o1, o2), true).subscribe(ts);

        ts.assertNoErrors();
        ts.assertCompleted();
        ts.assertValues(1, 3, 5, 7);
    }

    @Test
    public void testErrorInMiddle() {
        Observable<Integer> o1 = Observable.just(1, 3, 5, 7);
        Observable<Integer> o2 = Observable.just(2).concatWith(Observable.<Integer>error(new RuntimeException()));

        TestSubscriber<Integer> ts = new TestSubscriber<>();
        SortedMerge.create(Arrays.asList(o1, o2)).subscribe(ts);

        ts.assertError(RuntimeException.class);
        ts.assertNotCompleted();
        ts.assertValues(1, 2);
    }

    @Test
    public void testErrorImmediately() {
        Observable<Integer> o1 = Observable.just(1, 3, 5, 7);
        Observable<Integer> o2 = Observable.<Integer>error(new RuntimeException());

        TestSubscriber<Integer> ts = new TestSubscriber<>();
        SortedMerge.create(Arrays.asList(o1, o2)).subscribe(ts);

        ts.assertError(RuntimeException.class);
        ts.assertNotCompleted();
        ts.assertNoValues();
    }

    @Test
    public void testErrorInMiddleDelayed() {
        Observable<Integer> o1 = Observable.just(1, 3, 5, 7);
        Observable<Integer> o2 = Observable.just(2).concatWith(Observable.<Integer>error(new RuntimeException()));

        TestSubscriber<Integer> ts = new TestSubscriber<>();
        SortedMerge.create(Arrays.asList(o1, o2), true).subscribe(ts);

        ts.assertError(RuntimeException.class);
        ts.assertValues(1, 2, 3, 5, 7);
        ts.assertNotCompleted();
    }

    @Test
    public void testErrorImmediatelyDelayed() {
        Observable<Integer> o1 = Observable.just(1, 3, 5, 7);
        Observable<Integer> o2 = Observable.<Integer>error(new RuntimeException());

        TestSubscriber<Integer> ts = new TestSubscriber<>();
        SortedMerge.create(Arrays.asList(o1, o2), true).subscribe(ts);

        ts.assertError(RuntimeException.class);
        ts.assertNotCompleted();
        ts.assertValues(1, 3, 5, 7);
    }

    @Test
    public void testTake() {
        Observable<Integer> o1 = Observable.just(1, 3, 5, 7);
        Observable<Integer> o2 = Observable.just(2, 4, 6, 8);
        Observable<Integer> o3 = Observable.just(2, 4, 6, 8);

        TestSubscriber<Integer> ts = new TestSubscriber<>();
        SortedMerge.create(Arrays.asList(o1, o2, o3))
                .distinctUntilChanged((integer, integer2) -> integer.compareTo(integer2) != 0)
                .take(2)
                .subscribe(ts);

        ts.awaitTerminalEvent(1, TimeUnit.SECONDS);
        ts.assertNoErrors();
        ts.assertCompleted();
        ts.assertValues(1, 2);

    }
    @Test
    public void testBackpressure() {
        Observable<Integer> o1 = Observable.just(1, 3, 5, 7);
        Observable<Integer> o2 = Observable.just(2, 4, 6, 8);

        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        SortedMerge.create(Arrays.asList(o1, o2)).subscribe(ts);

        ts.requestMore(2);

        ts.assertNoErrors();
        ts.assertNotCompleted();
        ts.assertValues(1, 2);

    }
}