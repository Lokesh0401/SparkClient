package com.dremio.tools.iceberg.actions;

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.actions.Actions;
import org.apache.iceberg.actions.ExpireSnapshotsActionResult;
import org.junit.Assert;
import org.junit.Test;

public class SnapshotExpiryTest extends BaseAction {
  @Test
  public void expireOlderSnapshots() throws InterruptedException {
    table.newAppend()
      .appendFile(FILE_0)
      .appendFile(FILE_1)
      .commit();
    table.refresh();
    long count = 0;
    for (Snapshot snap : table.snapshots()) {
      count++;
    }
    Thread.sleep(1000 * 30);
    table.newAppend()
      .appendFile(FILE_2)
      .commit();
    table.refresh();
    long count1 = 0;
    for (Snapshot snapshot : table.snapshots()) {
      count1++;
    }
    long tsToExpire = System.currentTimeMillis() - 1000 * 20;
    Actions act = Actions.forTable(table);
    ExpireSnapshotsActionResult exa = act.expireSnapshots().expireOlderThan(tsToExpire).execute();
    table.refresh();
    long count2 = 0;
    for (Snapshot snapshot : table.snapshots()) {
      count2++;
    }
    Assert.assertEquals(1, count2);
  }
}
