Welcome to Pika (Multithreaded)
=========================================

This repository uses Pika as a base to create connections to AMQP brokers. Pika
doesn't use threads at all so it fails when using a consumer for long lived
tasks. This project builds on top of Pika to enable long lived task execution
for consumers by implementing a threading layer on top of the base Pika
connections.

Contents
========

.. todo::

   Add your own documents to the `docs` directory tree and insert them into
   the `toctree` table of contents.

.. toctree::
   :maxdepth: 2
   :titlesonly:
   :glob:

   Quick Start <README.md>
.. TODO: Your directories go here...

Recent Changes
==============

.. todo::

   Update the source changelog link.

See the `source changelog <https://github.com/nimbis/pika-multithreaded/commits/main>`_
for a complete change history.

.. git_changelog::
   :filename_filter: ^docs/.*$
   :detailed-message-pre: True

To-Do List
==========

The following to-do items exist across the document.

-----------------

.. todolist::

-----------------

Indices and tables
==================

* :ref:`genindex`
* :ref:`search`
