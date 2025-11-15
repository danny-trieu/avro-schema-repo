/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.apache.avro.repo;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Comprehensive unit tests for {@link InMemorySchemaEntryCache}
 */
public class TestInMemorySchemaEntryCache {

  private InMemorySchemaEntryCache cache;

  @Before
  public void setUp() {
    cache = new InMemorySchemaEntryCache();
  }

  @Test
  public void testLookupBySchemaReturnsNullWhenEmpty() {
    SchemaEntry result = cache.lookupBySchema("nonexistent");
    Assert.assertNull("Should return null for nonexistent schema", result);
  }

  @Test
  public void testLookupByIdReturnsNullWhenEmpty() {
    SchemaEntry result = cache.lookupById("nonexistent-id");
    Assert.assertNull("Should return null for nonexistent id", result);
  }

  @Test
  public void testAddAndLookupBySchema() {
    SchemaEntry entry = new SchemaEntry("id1", "schema1");
    SchemaEntry added = cache.add(entry);

    Assert.assertSame("Should return the same entry", entry, added);

    SchemaEntry found = cache.lookupBySchema("schema1");
    Assert.assertNotNull("Should find entry by schema", found);
    Assert.assertEquals("Should return correct entry", entry, found);
  }

  @Test
  public void testAddAndLookupById() {
    SchemaEntry entry = new SchemaEntry("id1", "schema1");
    cache.add(entry);

    SchemaEntry found = cache.lookupById("id1");
    Assert.assertNotNull("Should find entry by id", found);
    Assert.assertEquals("Should return correct entry", entry, found);
  }

  @Test
  public void testAddNullEntry() {
    SchemaEntry result = cache.add(null);
    Assert.assertNull("Adding null should return null", result);
  }

  @Test
  public void testAddDuplicateSchemaReturnsPriorEntry() {
    SchemaEntry entry1 = new SchemaEntry("id1", "schema1");
    SchemaEntry entry2 = new SchemaEntry("id2", "schema1"); // Same schema, different id

    SchemaEntry first = cache.add(entry1);
    SchemaEntry second = cache.add(entry2);

    Assert.assertSame("First add should return original entry", entry1, first);
    Assert.assertSame("Second add should return prior entry", entry1, second);
    Assert.assertNotSame("Should not return the new entry", entry2, second);
  }

  @Test
  public void testDuplicateSchemaDoesNotAddNewId() {
    SchemaEntry entry1 = new SchemaEntry("id1", "schema1");
    SchemaEntry entry2 = new SchemaEntry("id2", "schema1"); // Same schema, different id

    cache.add(entry1);
    cache.add(entry2);

    // When schema is duplicate, the new ID (id2) is NOT added to id map
    // Only the original entry's ID (id1) remains
    SchemaEntry foundById1 = cache.lookupById("id1");
    Assert.assertNotNull("Should find by id1", foundById1);
    Assert.assertEquals("Should map to the original entry", entry1, foundById1);

    SchemaEntry foundById2 = cache.lookupById("id2");
    Assert.assertNull("Should NOT find by id2 since duplicate schema", foundById2);
  }

  @Test
  public void testMultipleDistinctEntries() {
    SchemaEntry entry1 = new SchemaEntry("id1", "schema1");
    SchemaEntry entry2 = new SchemaEntry("id2", "schema2");
    SchemaEntry entry3 = new SchemaEntry("id3", "schema3");

    cache.add(entry1);
    cache.add(entry2);
    cache.add(entry3);

    Assert.assertEquals(entry1, cache.lookupBySchema("schema1"));
    Assert.assertEquals(entry2, cache.lookupBySchema("schema2"));
    Assert.assertEquals(entry3, cache.lookupBySchema("schema3"));

    Assert.assertEquals(entry1, cache.lookupById("id1"));
    Assert.assertEquals(entry2, cache.lookupById("id2"));
    Assert.assertEquals(entry3, cache.lookupById("id3"));
  }

  @Test
  public void testValuesReturnsAllEntries() {
    SchemaEntry entry1 = new SchemaEntry("id1", "schema1");
    SchemaEntry entry2 = new SchemaEntry("id2", "schema2");
    SchemaEntry entry3 = new SchemaEntry("id3", "schema3");

    cache.add(entry1);
    cache.add(entry2);
    cache.add(entry3);

    Iterable<SchemaEntry> values = cache.values();
    List<SchemaEntry> valueList = new ArrayList<SchemaEntry>();
    for (SchemaEntry entry : values) {
      valueList.add(entry);
    }

    Assert.assertEquals("Should have 3 entries", 3, valueList.size());
    Assert.assertTrue("Should contain entry1", valueList.contains(entry1));
    Assert.assertTrue("Should contain entry2", valueList.contains(entry2));
    Assert.assertTrue("Should contain entry3", valueList.contains(entry3));
  }

  @Test
  public void testValuesReturnsEmptyWhenCacheEmpty() {
    Iterable<SchemaEntry> values = cache.values();
    List<SchemaEntry> valueList = new ArrayList<SchemaEntry>();
    for (SchemaEntry entry : values) {
      valueList.add(entry);
    }

    Assert.assertEquals("Should be empty", 0, valueList.size());
  }

  @Test
  public void testValuesReturnsSnapshot() {
    SchemaEntry entry1 = new SchemaEntry("id1", "schema1");
    cache.add(entry1);

    Iterable<SchemaEntry> values = cache.values();

    // Add more entries after getting values
    cache.add(new SchemaEntry("id2", "schema2"));

    List<SchemaEntry> valueList = new ArrayList<SchemaEntry>();
    for (SchemaEntry entry : values) {
      valueList.add(entry);
    }

    Assert.assertEquals("Snapshot should only contain 1 entry", 1, valueList.size());
  }

  @Test
  public void testValuesOrderReflectsInsertionOrder() {
    SchemaEntry entry1 = new SchemaEntry("id1", "schema1");
    SchemaEntry entry2 = new SchemaEntry("id2", "schema2");
    SchemaEntry entry3 = new SchemaEntry("id3", "schema3");

    cache.add(entry1);
    cache.add(entry2);
    cache.add(entry3);

    Iterable<SchemaEntry> values = cache.values();
    List<SchemaEntry> valueList = new ArrayList<SchemaEntry>();
    for (SchemaEntry entry : values) {
      valueList.add(entry);
    }

    // LinkedList.push adds to the beginning, so order should be reversed
    Assert.assertEquals("First should be entry3", entry3, valueList.get(0));
    Assert.assertEquals("Second should be entry2", entry2, valueList.get(1));
    Assert.assertEquals("Third should be entry1", entry1, valueList.get(2));
  }

  @Test
  public void testFactoryCreatesNewInstance() {
    InMemorySchemaEntryCache.Factory factory = new InMemorySchemaEntryCache.Factory();
    SchemaEntryCache cache1 = factory.createSchemaEntryCache();
    SchemaEntryCache cache2 = factory.createSchemaEntryCache();

    Assert.assertNotNull("Should create cache", cache1);
    Assert.assertNotNull("Should create cache", cache2);
    Assert.assertNotSame("Should create distinct instances", cache1, cache2);
    Assert.assertTrue("Should create InMemorySchemaEntryCache",
        cache1 instanceof InMemorySchemaEntryCache);
  }

  @Test
  public void testConcurrentAdds() throws InterruptedException {
    final int threadCount = 10;
    final int entriesPerThread = 100;
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch doneLatch = new CountDownLatch(threadCount);
    final ExecutorService executor = Executors.newFixedThreadPool(threadCount);

    for (int i = 0; i < threadCount; i++) {
      final int threadId = i;
      executor.submit(new Runnable() {
        @Override
        public void run() {
          try {
            startLatch.await(); // Wait for all threads to be ready
            for (int j = 0; j < entriesPerThread; j++) {
              String id = "thread" + threadId + "-id" + j;
              String schema = "thread" + threadId + "-schema" + j;
              cache.add(new SchemaEntry(id, schema));
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          } finally {
            doneLatch.countDown();
          }
        }
      });
    }

    startLatch.countDown(); // Start all threads
    doneLatch.await(10, TimeUnit.SECONDS); // Wait for completion
    executor.shutdown();

    // Verify all entries were added
    for (int i = 0; i < threadCount; i++) {
      for (int j = 0; j < entriesPerThread; j++) {
        String id = "thread" + i + "-id" + j;
        String schema = "thread" + i + "-schema" + j;
        Assert.assertNotNull("Should find entry with id: " + id,
            cache.lookupById(id));
        Assert.assertNotNull("Should find entry with schema: " + schema,
            cache.lookupBySchema(schema));
      }
    }
  }

  @Test
  public void testConcurrentAddAndLookup() throws InterruptedException {
    final int threadCount = 10;
    final int operations = 100;
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch doneLatch = new CountDownLatch(threadCount * 2);
    final ExecutorService executor = Executors.newFixedThreadPool(threadCount * 2);
    final AtomicInteger successfulLookups = new AtomicInteger(0);

    // Pre-populate some entries
    for (int i = 0; i < 50; i++) {
      cache.add(new SchemaEntry("initial-id" + i, "initial-schema" + i));
    }

    // Writer threads
    for (int i = 0; i < threadCount; i++) {
      final int threadId = i;
      executor.submit(new Runnable() {
        @Override
        public void run() {
          try {
            startLatch.await();
            for (int j = 0; j < operations; j++) {
              String id = "writer" + threadId + "-id" + j;
              String schema = "writer" + threadId + "-schema" + j;
              cache.add(new SchemaEntry(id, schema));
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          } finally {
            doneLatch.countDown();
          }
        }
      });
    }

    // Reader threads
    for (int i = 0; i < threadCount; i++) {
      executor.submit(new Runnable() {
        @Override
        public void run() {
          try {
            startLatch.await();
            for (int j = 0; j < operations; j++) {
              // Try to lookup initial entries
              SchemaEntry entry = cache.lookupById("initial-id" + (j % 50));
              if (entry != null) {
                successfulLookups.incrementAndGet();
              }
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          } finally {
            doneLatch.countDown();
          }
        }
      });
    }

    startLatch.countDown();
    doneLatch.await(10, TimeUnit.SECONDS);
    executor.shutdown();

    Assert.assertTrue("Should have some successful lookups",
        successfulLookups.get() > 0);
  }

  @Test
  public void testConcurrentDuplicateSchemaAdds() throws InterruptedException {
    final int threadCount = 10;
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch doneLatch = new CountDownLatch(threadCount);
    final ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    final List<SchemaEntry> returnedEntries =
        new ArrayList<SchemaEntry>();

    for (int i = 0; i < threadCount; i++) {
      final int threadId = i;
      executor.submit(new Runnable() {
        @Override
        public void run() {
          try {
            startLatch.await();
            // All threads try to add the same schema with different IDs
            SchemaEntry entry = new SchemaEntry("id-" + threadId, "common-schema");
            SchemaEntry returned = cache.add(entry);
            synchronized (returnedEntries) {
              returnedEntries.add(returned);
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          } finally {
            doneLatch.countDown();
          }
        }
      });
    }

    startLatch.countDown();
    doneLatch.await(10, TimeUnit.SECONDS);
    executor.shutdown();

    // All returned entries should be the same (the first one that won)
    SchemaEntry first = returnedEntries.get(0);
    for (SchemaEntry entry : returnedEntries) {
      Assert.assertSame("All should return the same entry", first, entry);
    }

    // Only one entry should be in the schema map
    SchemaEntry found = cache.lookupBySchema("common-schema");
    Assert.assertNotNull("Should find by common schema", found);
    Assert.assertSame("Should be the winning entry", first, found);
  }
}
