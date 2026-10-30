/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.novarocks.test;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.DataNode;

/** Private, one-run test child. The Rust ManagedProcess owns final termination. */
public final class NovaMiniDfs {
  private static void publish(Path path, String text) throws Exception {
    Path temporary = path.resolveSibling(path.getFileName() + ".new");
    Files.writeString(temporary, text, StandardCharsets.UTF_8);
    Files.move(temporary, path, StandardCopyOption.ATOMIC_MOVE);
  }

  private static int loopback(InetSocketAddress address) {
    if (address.getAddress() == null || !address.getAddress().isLoopbackAddress()
        || address.getPort() <= 0) {
      throw new IllegalStateException("Listener is not bound to loopback: " + address);
    }
    return address.getPort();
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2 || !args[1].matches("[a-zA-Z0-9_-]{1,64}")) {
      throw new IllegalArgumentException("Expected absolute private run directory and run token");
    }
    Path run = Path.of(args[0]);
    if (!run.isAbsolute()) throw new IllegalArgumentException("Run directory must be absolute");
    Files.createDirectories(run.resolve("conf"));
    String token = args[1];
    String root = "/fixture/" + token;
    Configuration conf = new HdfsConfiguration();
    conf.set("hadoop.security.authentication", "simple");
    conf.set("dfs.http.policy", "HTTP_ONLY");
    conf.set("dfs.namenode.rpc-address", "127.0.0.1:0");
    conf.set("dfs.namenode.http-address", "127.0.0.1:0");
    conf.set("dfs.datanode.address", "127.0.0.1:0");
    conf.set("dfs.datanode.http.address", "127.0.0.1:0");
    conf.set("dfs.datanode.ipc.address", "127.0.0.1:0");
    conf.set("dfs.client.use.datanode.hostname", "false");
    conf.setInt("dfs.replication", 1);
    conf.setBoolean("dfs.permissions.enabled", true);
    AtomicReference<MiniDFSCluster> owned = new AtomicReference<>();
    Thread shutdown = new Thread(() -> {
      MiniDFSCluster cluster = owned.getAndSet(null);
      if (cluster != null) cluster.shutdown();
    }, "private-minidfs-shutdown");
    Runtime.getRuntime().addShutdownHook(shutdown);
    try {
      // MiniDFS can wait internally. The external monitor's absolute lifetime bounds this child.
      MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf, run.resolve("data").toFile())
          .nameNodePort(0).nameNodeHttpPort(0).numDataNodes(1).storagesPerDatanode(1)
          .setDnHttpPorts(0).setDnIpcPorts(0).checkDataNodeAddrConfig(true).build();
      owned.set(cluster);
      cluster.waitActive();
      if (cluster.getDataNodes().size() != 1) throw new IllegalStateException("Expected one DN");
      InetSocketAddress rpcAddress = cluster.getNameNode().getNameNodeAddress();
      int rpc = loopback(rpcAddress);
      int http = loopback(cluster.getNameNode().getHttpAddress());
      DataNode dn = cluster.getDataNodes().get(0);
      int xfer = loopback(dn.getXferAddress());
      int ipc = dn.getIpcPort();
      int dnHttp = dn.getInfoPort();
      if (ipc <= 0 || dnHttp <= 0) throw new IllegalStateException("DN listeners not bound");
      // IPC/HTTP host comes from the explicit loopback configuration above; Rust probes actual ports.
      URI reported = cluster.getURI();
      if (!reported.getScheme().equals("hdfs") || reported.getPort() != rpc) {
        throw new IllegalStateException("NameNode URI disagrees with its bound RPC port: " + reported);
      }
      // getURI may reverse-resolve the bound address to localhost. Freeze the
      // physical route from the actual socket, without guessing a free port.
      URI physical = new URI("hdfs", null, rpcAddress.getAddress().getHostAddress(),
          rpc, null, null, null);
      Configuration client = new Configuration(false);
      client.set("hadoop.security.authentication", "simple");
      client.set("fs.defaultFS", physical.toString());
      client.set("dfs.ha.namenodes.nr-fixture", "nn0");
      client.set("dfs.namenode.rpc-address.nr-fixture.nn0", "127.0.0.1:" + rpc);
      client.set("dfs.client.failover.proxy.provider.nr-fixture",
          "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
      try (OutputStream output = Files.newOutputStream(run.resolve("conf/core-site.xml"))) {
        client.writeXml(output);
      }
      try (FileSystem admin = cluster.getFileSystem()) {
        org.apache.hadoop.fs.Path base = new org.apache.hadoop.fs.Path(root);
        admin.mkdirs(base);
        admin.setPermission(base, new FsPermission((short) 0777));
        publish(run.resolve("ready"), "READY=" + token + "\nphysical=" + physical
            + "\nreported=" + reported
            + "\nlogical=hdfs://nr-fixture\nroot=" + root + "\nports="
            + rpc + "," + http + "," + xfer + "," + ipc + "," + dnHttp + "\n");
        long until = System.nanoTime() + Duration.ofSeconds(180).toNanos();
        while (!Files.exists(run.resolve("stop"))) {
          if (System.nanoTime() >= until) throw new IllegalStateException("Private stop deadline expired");
          Thread.sleep(25);
        }
        String stop = Files.readString(run.resolve("stop"), StandardCharsets.UTF_8);
        if (stop.equals("VERIFY")) {
          StringBuilder witness = new StringBuilder("RUN=" + token + "\n");
          for (String route : new String[] {"physical", "logical"}) {
            String owner = admin.getFileStatus(new org.apache.hadoop.fs.Path(
                root + "/" + route + "/owner-witness")).getOwner();
            if (!owner.equals("alice")) throw new IllegalStateException("Wrong effective user: " + owner);
            witness.append(route).append("=").append(owner).append('\n');
          }
          publish(run.resolve("owner-witness"), witness.toString());
        } else if (!stop.equals("ABORT")) {
          throw new IllegalStateException("Invalid private stop request");
        }
      }
    } finally {
      MiniDFSCluster cluster = owned.getAndSet(null);
      if (cluster != null) cluster.shutdown();
      Runtime.getRuntime().removeShutdownHook(shutdown);
    }
  }
}
