/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package test.org.apache.spark.sql;

import java.io.Serializable;
import java.util.*;

import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.GroupedDataset;
import scala.Tuple2;
import org.junit.*;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.catalyst.encoders.Encoder;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.catalyst.encoders.Encoder$;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.test.TestSQLContext;
import static org.apache.spark.sql.functions.*;

public class JavaDatasetSuite implements Serializable {
  private transient JavaSparkContext jsc;
  private transient TestSQLContext context;

  @Before
  public void setUp() {
    // Trigger static initializer of TestData
    SparkContext sc = new SparkContext("local[*]", "testing");
    jsc = new JavaSparkContext(sc);
    context = new TestSQLContext(sc);
    context.loadTestData();
  }

  @After
  public void tearDown() {
    context.sparkContext().stop();
    context = null;
    jsc = null;
  }

  @Test
  public void testCommonOperation() {
    List<String> data = Arrays.asList("hello", "world");
    Dataset<String> ds = context.createDataset(data, Encoder$.MODULE$.forString());
    Assert.assertEquals("hello", ds.first());

    Dataset<String> filtered = ds.filter(new Function<String, Boolean>() {
      @Override
      public Boolean call(String v) throws Exception {
        return v.startsWith("h");
      }
    });
    Assert.assertEquals(Arrays.asList("hello"), filtered.jcollect());


    Dataset<Integer> mapped = ds.map(new Function<String, Integer>() {
      @Override
      public Integer call(String v) throws Exception {
        return v.length();
      }
    }, Encoder$.MODULE$.forInt());
    Assert.assertEquals(Arrays.asList(5, 5), mapped.jcollect());

    Dataset<String> parMapped = ds.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
      @Override
      public Iterable<String> call(Iterator<String> it) throws Exception {
        List<String> ls = new LinkedList<String>();
        while (it.hasNext()) {
          ls.add(it.next().toUpperCase());
        }
        return ls;
      }
    }, Encoder$.MODULE$.forString());
    Assert.assertEquals(Arrays.asList("HELLO", "WORLD"), parMapped.jcollect());

    Dataset<String> flatMapped = ds.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterable<String> call(String s) throws Exception {
        List<String> ls = new LinkedList<String>();
        for (char c : s.toCharArray()) {
          ls.add(String.valueOf(c));
        }
        return ls;
      }
    }, Encoder$.MODULE$.forString());
    Assert.assertEquals(
      Arrays.asList("h", "e", "l", "l", "o", "w", "o", "r", "l", "d"),
      flatMapped.jcollect());
  }

  @Test
  public void testForeach() {
    final Accumulator<Integer> accum = jsc.accumulator(0);
    List<String> data = Arrays.asList("a", "b", "c");
    Dataset<String> ds = context.createDataset(data, Encoder$.MODULE$.forString());

    ds.foreach(new VoidFunction<String>() {
      @Override
      public void call(String s) throws Exception {
        accum.add(1);
      }
    });
    Assert.assertEquals(3, accum.value().intValue());
  }

  @Test
  public void testReduce() {
    List<Integer> data = Arrays.asList(1, 2, 3);
    Dataset<Integer> ds = context.createDataset(data, Encoder$.MODULE$.forInt());

    int reduced = ds.reduce(new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer v1, Integer v2) throws Exception {
        return v1 + v2;
      }
    });
    Assert.assertEquals(6, reduced);

    int folded = ds.fold(1, new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer v1, Integer v2) throws Exception {
        return v1 * v2;
      }
    });
    Assert.assertEquals(6, folded);
  }

  @Test
  public void testGroupBy() {
    List<String> data = Arrays.asList("a", "foo", "bar");
    Dataset<String> ds = context.createDataset(data, Encoder$.MODULE$.forString());
    GroupedDataset<Integer, String> grouped = ds.groupBy(new Function<String, Integer>() {
      @Override
      public Integer call(String v) throws Exception {
        return v.length();
      }
    }, Encoder$.MODULE$.forInt());

    Dataset<String> mapped = grouped.mapGroups(
      new Function2<Integer, Iterator<String>, Iterator<String>>() {
        @Override
        public Iterator<String> call(Integer key, Iterator<String> data) throws Exception {
          StringBuilder sb = new StringBuilder(key.toString());
          while (data.hasNext()) {
            sb.append(data.next());
          }
          return Collections.singletonList(sb.toString()).iterator();
        }
      },
      Encoder$.MODULE$.forString());

    Assert.assertEquals(Arrays.asList("1a", "3foobar"), mapped.jcollect());

    List<Integer> data2 = Arrays.asList(2, 6, 10);
    Dataset<Integer> ds2 = context.createDataset(data2, Encoder$.MODULE$.forInt());
    GroupedDataset<Integer, Integer> grouped2 = ds2.groupBy(new Function<Integer, Integer>() {
      @Override
      public Integer call(Integer v) throws Exception {
        return v / 2;
      }
    }, Encoder$.MODULE$.forInt());

    Dataset<String> cogrouped = grouped.cogroup(
      grouped2,
      new Function3<Integer, Iterator<String>, Iterator<Integer>, Iterator<String>>() {
        @Override
        public Iterator<String> call(
            Integer key,
            Iterator<String> left,
            Iterator<Integer> right) throws Exception {
          StringBuilder sb = new StringBuilder(key.toString());
          while (left.hasNext()) {
            sb.append(left.next());
          }
          sb.append("#");
          while (right.hasNext()) {
            sb.append(right.next());
          }
          return Collections.singletonList(sb.toString()).iterator();
        }
      },
      Encoder$.MODULE$.forString());

    Assert.assertEquals(Arrays.asList("1a#2", "3foobar#6", "5#10"), cogrouped.jcollect());
  }

  @Test
  public void testSelect() {
    List<Integer> data = Arrays.asList(2, 6);
    Dataset<Integer> ds = context.createDataset(data, Encoder$.MODULE$.forInt());

    Dataset<Tuple2<Integer, String>> selected = ds.select(
      expr("value + 1").as(Encoder$.MODULE$.forInt()),
      col("value").cast("string").as(Encoder$.MODULE$.forString()));

    Assert.assertEquals(
      Arrays.asList(new Tuple2<Integer, String>(3, "2"), new Tuple2<Integer, String>(7, "6")),
      selected.jcollect());
  }

  @Test
  public void testSetOperation() {
    List<String> data = Arrays.asList("abc", "abc", "xyz");
    Dataset<String> ds = context.createDataset(data, Encoder$.MODULE$.forString());

    Assert.assertEquals(
      Arrays.asList("abc", "xyz"),
      sort(ds.distinct().jcollect().toArray(new String[0])));

    List<String> data2 = Arrays.asList("xyz", "foo", "foo");
    Dataset<String> ds2 = context.createDataset(data2, Encoder$.MODULE$.forString());

    Dataset<String> intersected = ds.intersect(ds2);
    Assert.assertEquals(Arrays.asList("xyz"), intersected.jcollect());

    Dataset<String> unioned = ds.union(ds2);
    Assert.assertEquals(
      Arrays.asList("abc", "abc", "foo", "foo", "xyz", "xyz"),
      sort(unioned.jcollect().toArray(new String[0])));

    Dataset<String> subtracted = ds.subtract(ds2);
    Assert.assertEquals(Arrays.asList("abc", "abc"), subtracted.jcollect());
  }

  private <T extends Comparable<T>> List<T> sort(T[] data) {
    Arrays.sort(data);
    return Arrays.asList(data);
  }

  @Test
  public void testJoin() {
    List<Integer> data = Arrays.asList(1, 2, 3);
    Dataset<Integer> ds = context.createDataset(data, Encoder$.MODULE$.forInt()).as("a");
    List<Integer> data2 = Arrays.asList(2, 3, 4);
    Dataset<Integer> ds2 = context.createDataset(data2, Encoder$.MODULE$.forInt()).as("b");

    Dataset<Tuple2<Integer, Integer>> joined =
      ds.joinWith(ds2, col("a.value").equalTo(col("b.value")));
    Assert.assertEquals(
      Arrays.asList(new Tuple2<Integer, Integer>(2, 2), new Tuple2<Integer, Integer>(3, 3)),
      joined.jcollect());
  }

  @Test
  public void testTuple2Encoder() {
    Encoder<Tuple2<Integer, String>> encoder =
      Encoder$.MODULE$.forTuple2(Integer.class, String.class);
    List<Tuple2<Integer, String>> data =
      Arrays.<Tuple2<Integer, String>>asList(new Tuple2(1, "a"), new Tuple2(2, "b"));
    Dataset<Tuple2<Integer, String>> ds = context.createDataset(data, encoder);

    Dataset<String> mapped = ds.map(new Function<Tuple2<Integer, String>, String>() {
      @Override
      public String call(Tuple2<Integer, String> v) throws Exception {
        return v._2();
      }
    }, Encoder$.MODULE$.forString());
    Assert.assertEquals(Arrays.asList("a", "b"), mapped.jcollect());
  }
}
