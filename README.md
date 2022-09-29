# Pika (Multithreaded)

## Introduction

This repository uses Pika as a base to create connections to AMQP brokers. Pika
doesn't use threads at all so it fails when using a consumer for long lived
tasks. This project builds on top of Pika to enable long lived task execution
for consumers by implementing a threading layer on top of the base Pika
connections.
