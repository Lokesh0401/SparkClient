package com.dremio.tools.iceberg.actions;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.Actions;
import org.apache.iceberg.actions.RewriteManifestsActionResult;
import org.junit.Test;

public class RewriteManifestFilesDemo extends BaseAction {
  @Test
  public void reducingNoOfManifestFiles() {
    table.newAppend()
      .appendFile(FILE_0)
      .appendFile(FILE_1)
      .commit();

    table.newAppend()
      .appendFile(FILE_2)
      .appendFile(FILE_3)
      .commit();

    List<ManifestFile> manifests = table.currentSnapshot().allManifests();
    //Assert.assertEquals(2,manifests.size());
    table.updateProperties()
      .set(TableProperties.MANIFEST_TARGET_SIZE_BYTES, String.valueOf((manifests.get(0).length() * 3)))
      .commit();
    Actions act = Actions.forTable(table);
    RewriteManifestsActionResult result = act.rewriteManifests()
      .rewriteIf(manifestFile -> true)
      .execute();
    System.out.println("Manifests rewritten with result: " + result);
    table.refresh();
    manifests = table.currentSnapshot().allManifests();
    assertEquals(1, manifests.size());
  }
}