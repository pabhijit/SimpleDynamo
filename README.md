# SimpleDynamo

This is a simplified version of Dynamo-style key-value storage. There are three main pieces that are implemented: 1) Partitioning, 2) Replication, and 3) Failure handling.

The main goal is to provide both availability and linearizability at the same time. In other words, this implementation always perform read and write operations successfully even under failures. At the same time, a read operation should always return the most recent value.
