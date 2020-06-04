package org.notmysock.benchmark.tpcds;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.util.StringUtils;

public final class TpcdsUtils {

  @SuppressWarnings("SameParameterValue")
  static void copyDependencyJars(Configuration conf, Class<?>... classes)
      throws IOException {
    Set<String> jars = new HashSet<>();
    FileSystem localFs = FileSystem.getLocal(conf);
    jars.addAll(conf.getStringCollection("tmpjars"));
    jars.addAll(Arrays.stream(classes).filter(Objects::nonNull).map(clazz -> {
      String path = Utilities.jarFinderGetJar(clazz);
      if (path == null) {
        throw new RuntimeException("Could not find jar for class " + clazz
            + " in order to ship it to the cluster.");
      }
      try {
        if (!localFs.exists(new Path(path))) {
          throw new RuntimeException(
              "Could not validate jar file " + path + " for class " + clazz);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return path;
    }).collect(Collectors.toList()));

    if (jars.isEmpty()) {
      return;
    }
    conf.set("tmpjars", StringUtils.arrayToString(jars.toArray(new String[0])));
  }
}
