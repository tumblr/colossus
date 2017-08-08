# Tasks

A task is a way to run arbitrary functions inside a worker.  This is very
useful if you need to spin up some clients outside the context of the server.

#### Basic Task

Starting a simple task is very easy

@@snip [Tasks.scala](../scala/Tasks.scala) { #example }

The way this works is the task binds to one of the workers in the IO System.
The worker then executes the task inside its event loop and will continue to
keep track of it until the task calls the `unbind()` method.

#### Task Proxy Actors

The reason tasks are bound to workers is because like connection handlers,
tasks can hook into the worker's mailbox to receive messages from outside the
event loop.  Every task has a built-in proxy actor with external code can use
to communicate with the task 

Just like an Akka actor, tasks have access to `self`, `sender`, and `become`
members.  A Task will automatically be unbound if its proxy actor is stopped,
and vise versa.

#### A Word of Caution

Because tasks can send and receive messages, they behave a bit like actors.
However it's important to realize that tasks are not actors and do not have the
same fault-tolerance and supervision features that an Actor has.  There's
nothing to prevent a task from running CPU intensive code inside a worker and
gumming up its event loop.  Also workers only "watch" tasks in the context of
forwarding messages to them.  Tasks have no concept of lifecycle, they cannot
be killed, and if a task throws an exception it may kill the whole worker.

For these reasons, if you are building a system that is running a server and
also needs to run background tasks, it is better to run the tasks in a separate
IO System from the server.  If you're only running 1 task, then that IO System
only needs 1 worker.

A good pattern for tasks is to pair them with an actor.  The actor creates the
task and periodically sends status check requests to it.  If the supervisor
actor fails to receive a response in time, it can simply destroy the entire IO
System and start over (remember an IO System with 1 worker is only 2 actors and
1 thread, so they are fairly lightweight)
