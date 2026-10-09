"use client";

import { useEffect, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Separator } from "@/components/ui/separator";

interface StorageNode {
  id: string;
  url: string;
  alive: boolean;
  consecutive_failures: number;
  down_for_secs: number | null;
  latency_ms: number | null;
  chunks: number;
  bytes: number;
}

interface ClusterInfo {
  served_by: string;
  this_node_is_leader: boolean;
  leader: string | null;
  replication_factor: number;
  write_quorum: number;
  chunk_size: number;
  parallelism: number;
  alive_storage_nodes: number;
  total_storage_nodes: number;
  storage_nodes: StorageNode[];
  metadata: {
    files: number;
    chunks: number;
    replicas: number;
    under_replicated: number;
  };
}

const formatBytes = (n: number) => {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KiB`;
  return `${(n / 1024 / 1024).toFixed(2)} MiB`;
};

export const ClusterStatus = () => {
  const [info, setInfo] = useState<ClusterInfo | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [lastServers, setLastServers] = useState<string[]>([]);

  useEffect(() => {
    let cancelled = false;

    const load = async () => {
      try {
        const res = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/cluster/status`, {
          cache: "no-store",
        });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data: ClusterInfo = await res.json();
        if (!cancelled) {
          setInfo(data);
          setError(null);
          // remember which API nodes answered recently => shows the load balancer at work
          setLastServers((prev) => [data.served_by, ...prev].slice(0, 8));
        }
      } catch (e) {
        if (!cancelled) setError(e instanceof Error ? e.message : "cannot reach the cluster");
      }
    };

    load();
    const timer = setInterval(load, 2000);
    return () => {
      cancelled = true;
      clearInterval(timer);
    };
  }, []);

  if (error && !info) {
    return <p className="text-red-600">Cluster unreachable: {error}</p>;
  }
  if (!info) {
    return <p>Loading cluster status…</p>;
  }

  return (
    <div className="space-y-4">
      <Card>
        <CardHeader>
          <CardTitle>Distributed cluster overview</CardTitle>
        </CardHeader>
        <Separator />
        <CardContent className="grid grid-cols-2 gap-4 p-4 md:grid-cols-4">
          <Stat label="Answered by API node" value={info.served_by} />
          <Stat label="Leader (background jobs)" value={info.leader ?? "—"} />
          <Stat
            label="Storage nodes alive"
            value={`${info.alive_storage_nodes} / ${info.total_storage_nodes}`}
          />
          <Stat
            label="Replication R / write quorum W"
            value={`${info.replication_factor} / ${info.write_quorum}`}
          />
          <Stat label="Files" value={String(info.metadata.files)} />
          <Stat label="Chunks" value={String(info.metadata.chunks)} />
          <Stat label="Replica copies" value={String(info.metadata.replicas)} />
          <Stat
            label="Under-replicated chunks"
            value={String(info.metadata.under_replicated)}
            bad={info.metadata.under_replicated > 0}
          />
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Storage nodes</CardTitle>
        </CardHeader>
        <Separator />
        <CardContent className="p-4">
          <table className="w-full text-left text-sm">
            <thead>
              <tr className="border-b">
                <th className="py-2">Node</th>
                <th>Status</th>
                <th>Latency</th>
                <th>Chunks stored</th>
                <th>Data</th>
              </tr>
            </thead>
            <tbody>
              {info.storage_nodes.map((n) => (
                <tr key={n.id} className="border-b last:border-0">
                  <td className="py-2 font-mono">{n.id}</td>
                  <td>
                    <span
                      className={`rounded px-2 py-0.5 text-xs font-semibold text-white ${
                        n.alive ? "bg-green-600" : "bg-red-600"
                      }`}
                    >
                      {n.alive ? "UP" : `DOWN${n.down_for_secs != null ? ` (${n.down_for_secs}s)` : ""}`}
                    </span>
                  </td>
                  <td>{n.latency_ms != null ? `${n.latency_ms} ms` : "—"}</td>
                  <td>{n.chunks}</td>
                  <td>{formatBytes(n.bytes)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Load balancing: last API nodes that answered</CardTitle>
        </CardHeader>
        <Separator />
        <CardContent className="flex flex-wrap gap-2 p-4">
          {lastServers.map((s, i) => (
            <span key={i} className="rounded bg-slate-900 px-2 py-1 font-mono text-xs text-white">
              {s}
            </span>
          ))}
        </CardContent>
      </Card>
    </div>
  );
};

const Stat = ({ label, value, bad }: { label: string; value: string; bad?: boolean }) => (
  <div>
    <div className="text-xs uppercase text-slate-500">{label}</div>
    <div className={`text-xl font-semibold ${bad ? "text-red-600" : ""}`}>{value}</div>
  </div>
);
